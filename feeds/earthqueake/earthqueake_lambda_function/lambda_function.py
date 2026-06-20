import json
import logging
import os
import time
from datetime import datetime, timezone

import boto3
import requests

logger = logging.getLogger()
logger.setLevel(logging.INFO)

USGS_URL = "https://earthquake.usgs.gov/fdsnws/event/1/query"
S3_BUCKET = os.environ.get("S3_BUCKET", "openweathermap-thim")
S3_PREFIX = os.environ.get("S3_PREFIX", "earthquake")
DYNAMODB_TABLE = os.environ["DYNAMODB_TABLE"]
MAX_FETCH_RETRIES = 3
BACKOFF_BASE = 2  # seconds

s3 = boto3.client("s3")
dynamodb = boto3.resource("dynamodb")


# ─────────────────────────────────────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────────────────────────────────────

def _s3_key(run_id: str, phase: str, page: int, starttime: str) -> str:
    # Partition by the date of the data being fetched, not the processing date.
    # starttime format: "YYYY-MM-DDTHH:MM:SS" (no timezone suffix from dispatcher)
    data_date = datetime.strptime(starttime[:10], "%Y-%m-%d")
    return (
        f"{S3_PREFIX}/"
        f"year={data_date:%Y}/month={data_date:%m}/day={data_date:%d}/"
        f"run={run_id}/{phase}/page={page:05d}.json"
    )


def _set_checkpoint(table, pk: str, sk: str, status: str, **attrs) -> None:
    set_expr = "SET #st = :s, updatedAt = :t"
    names = {"#st": "status"}
    values = {":s": status, ":t": datetime.now(timezone.utc).isoformat()}
    for k, v in attrs.items():
        set_expr += f", #{k} = :{k}"
        names[f"#{k}"] = k
        values[f":{k}"] = v
    table.update_item(
        Key={"pk": pk, "sk": sk},
        UpdateExpression=set_expr,
        ExpressionAttributeNames=names,
        ExpressionAttributeValues=values,
    )


def _fetch_with_backoff(starttime: str, endtime: str, page_size: int, offset: int) -> dict:
    params = {
        "format": "geojson",
        "starttime": starttime,
        "endtime": endtime,
        "limit": page_size,
        "offset": offset,
        "orderby": "time",
    }
    last_exc: Exception | None = None
    for attempt in range(MAX_FETCH_RETRIES):
        try:
            resp = requests.get(USGS_URL, params=params, timeout=30)
            if resp.status_code != 200:
                raise RuntimeError(f"USGS HTTP {resp.status_code}: {resp.text[:300]}")
            return resp.json()
        except Exception as exc:
            last_exc = exc
            if attempt < MAX_FETCH_RETRIES - 1:
                wait = BACKOFF_BASE ** attempt
                logger.warning(
                    "Fetch attempt %d/%d failed: %s – retrying in %ds",
                    attempt + 1, MAX_FETCH_RETRIES, exc, wait,
                )
                time.sleep(wait)
    raise last_exc  # type: ignore[misc]


# ─────────────────────────────────────────────────────────────────────────────
# Handler – SQS trigger, reports partial batch failures
# ─────────────────────────────────────────────────────────────────────────────

def lambda_handler(event: dict, context) -> dict:
    table = dynamodb.Table(DYNAMODB_TABLE)
    failed_items: list[dict] = []

    for record in event["Records"]:
        message_id: str = record["messageId"]
        try:
            _process(table, json.loads(record["body"]))
        except Exception as exc:
            logger.error("Failed messageId=%s: %s", message_id, exc)
            failed_items.append({"itemIdentifier": message_id})

    return {"batchItemFailures": failed_items}


def _process(table, msg: dict) -> None:
    run_id: str = msg["runId"]
    phase: str = msg["phase"]
    page: int = int(msg["page"])
    starttime: str = msg["starttime"]   # day-specific, e.g. "2025-01-15T00:00:00"
    endtime: str = msg["endtime"]
    page_size: int = int(msg["pageSize"])
    offset: int = int(msg["offset"])
    # "date" added by dispatcher; fall back to starttime[:10] for older messages
    date_str: str = msg.get("date") or starttime[:10]

    pk = f"RUN#{run_id}"
    sk = f"PAGE#{phase}#{date_str}#{page:05d}"  # unique per day+page

    logger.info("Worker runId=%s phase=%s page=%d offset=%d", run_id, phase, page, offset)

    # ── Idempotency check – short-circuit if already SUCCESS ──────────────────
    existing = table.get_item(Key={"pk": pk, "sk": sk}).get("Item")
    if existing and existing.get("status") == "SUCCESS":
        logger.info("Checkpoint SUCCESS – skipping runId=%s page=%d", run_id, page)
        return

    # ── Mark RUNNING ──────────────────────────────────────────────────────────
    _set_checkpoint(table, pk, sk, "RUNNING", runId=run_id, phase=phase, page=page)

    # ── Fetch page from USGS with exponential backoff ─────────────────────────
    try:
        payload = _fetch_with_backoff(starttime, endtime, page_size, offset)
    except Exception as exc:
        logger.error("USGS fetch failed runId=%s page=%d: %s", run_id, page, exc)
        _set_checkpoint(table, pk, sk, "FAILED", errorMessage=str(exc)[:1000])
        raise  # SQS will retry; after maxReceiveCount it routes to DLQ

    feature_count = len(payload.get("features", []))
    payload["_meta"] = {
        "runId": run_id,
        "phase": phase,
        "page": page,
        "offset": offset,
        "featureCount": feature_count,
        "ingestTimestamp": datetime.now(timezone.utc).isoformat(),
        "source": USGS_URL,
    }

    # ── Write raw response to S3 ──────────────────────────────────────────────
    s3_key = _s3_key(run_id, phase, page, starttime)
    try:
        s3.put_object(
            Bucket=S3_BUCKET,
            Key=s3_key,
            Body=json.dumps(payload, ensure_ascii=False).encode("utf-8"),
            ContentType="application/json",
        )
    except Exception as exc:
        logger.error("S3 write failed runId=%s page=%d: %s", run_id, page, exc)
        _set_checkpoint(table, pk, sk, "FAILED", errorMessage=str(exc)[:1000])
        raise

    # ── Mark SUCCESS – triggers DynamoDB stream → completion detector ─────────
    _set_checkpoint(table, pk, sk, "SUCCESS", s3Key=s3_key, featureCount=feature_count)
    logger.info("SUCCESS runId=%s page=%d features=%d s3=%s", run_id, page, feature_count, s3_key)
