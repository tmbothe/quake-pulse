import json
import logging
import math
import os
from datetime import datetime, timezone, timedelta

import boto3
from boto3.dynamodb.conditions import Key, Attr
import requests

logger = logging.getLogger()
logger.setLevel(logging.INFO)

USGS_URL = "https://earthquake.usgs.gov/fdsnws/event/1/query"
USGS_COUNT_URL = "https://earthquake.usgs.gov/fdsnws/event/1/count"
SQS_QUEUE_URL = os.environ["SQS_QUEUE_URL"]
DYNAMODB_TABLE = os.environ["DYNAMODB_TABLE"]
DEFAULT_PAGE_SIZE = int(os.environ.get("PAGE_SIZE", "1000"))

sqs = boto3.client("sqs")
dynamodb = boto3.resource("dynamodb")
sfn = boto3.client("stepfunctions")


def lambda_handler(event: dict, context) -> dict:
    task_token: str = event["taskToken"]
    exec_time: str = event.get("execTime", "")
    inp: dict = event.get("input", {})

    table = dynamodb.Table(DYNAMODB_TABLE)

    # ── Determine run mode ─────────────────────────────────────────────────────
    # NEW RUN  : no runId in input → increment global counter, start from page 1.
    # RESUME   : runId provided    → reuse existing run, skip SUCCESS pages,
    #            optional fromPage skips pages below that number for every day.
    #
    # Manual resume example input:
    #   {"runId": "000003", "fromPage": 5,
    #    "starttime": "2025-06-01T00:00:00", "endtime": "2025-06-30T23:59:59"}
    # ──────────────────────────────────────────────────────────────────────────
    resume_run_id: str = inp.get("runId", "").strip()
    from_page: int = max(1, int(inp.get("fromPage", 1)))

    if resume_run_id:
        run_id, completed_pages, total_pages_meta = _load_resume_state(
            table, resume_run_id
        )
        logger.info(
            "RESUME runId=%s fromPage=%d alreadySuccess=%d previousTotalPages=%d",
            run_id, from_page, len(completed_pages), total_pages_meta,
        )
    else:
        run_id = _next_run_id(table)
        from_page = 1
        completed_pages: set[tuple[str, int]] = set()
        total_pages_meta = 0  # will be set after counting
        logger.info("NEW run runId=%s", run_id)

    page_size: int = int(inp.get("pageSize", DEFAULT_PAGE_SIZE))
    phase: str = "fetch"

    starttime, endtime = _resolve_window(inp)

    logger.info(
        "runId=%s execTime=%s starttime=%s endtime=%s pageSize=%d",
        run_id, exec_time, starttime, endtime, page_size,
    )

    # ── 1. Split window into per-day ranges; query count for each day ─────────
    day_ranges = _generate_day_ranges(starttime, endtime)
    logger.info("Window spans %d day(s)", len(day_ranges))

    # (day_start, day_end, event_count, page_count)
    day_infos: list[tuple[str, str, int, int]] = []
    total_pages: int = 0

    for day_start, day_end in day_ranges:
        count = _get_day_count(day_start, day_end)
        pages = max(1, math.ceil(count / page_size)) if count > 0 else 0
        day_infos.append((day_start, day_end, count, pages))
        total_pages += pages
        logger.info("date=%s count=%d pages=%d", day_start[:10], count, pages)

    # On resume keep the original totalPages so the detector threshold is stable.
    # On a new run totalPages comes from the USGS counts above.
    effective_total_pages = total_pages_meta if resume_run_id else total_pages

    # ── 2. Persist / update run metadata ──────────────────────────────────────
    table.put_item(
        Item={
            "pk": f"RUN#{run_id}",
            "sk": "META",
            "runId": run_id,
            "taskToken": task_token,       # updated on every resume
            "totalPages": effective_total_pages,
            "phase": phase,
            "starttime": starttime,
            "endtime": endtime,
            "pageSize": page_size,
            "execTime": exec_time,
            "status": "DISPATCHING",
            "createdAt": datetime.now(timezone.utc).isoformat(),
        }
    )

    # No data in the requested window – signal success immediately
    if effective_total_pages == 0:
        logger.info("No events found for runId=%s – signalling success", run_id)
        sfn.send_task_success(
            taskToken=task_token,
            output=json.dumps({"runId": run_id, "totalPages": 0, "status": "SUCCESS"}),
        )
        return {"runId": run_id, "totalPages": 0, "dispatched": 0}

    # ── 3. Fan out one SQS message per (day, page), honouring skip rules ──────
    batch: list = []
    dispatched: int = 0
    skipped_success: int = 0
    skipped_before: int = 0

    for day_start, day_end, count, pages in day_infos:
        if pages == 0:
            continue
        date_str = day_start[:10]  # "YYYY-MM-DD"

        for page in range(1, pages + 1):

            # Skip pages before fromPage (manual restart threshold)
            if page < from_page:
                skipped_before += 1
                continue

            # Skip pages already completed in a previous attempt
            if (date_str, page) in completed_pages:
                skipped_success += 1
                logger.debug("Skip SUCCESS date=%s page=%d", date_str, page)
                continue

            batch.append(
                {
                    # ID must be unique within the batch (max 80 chars)
                    "Id": f"d{date_str.replace('-', '')}p{page}",
                    "MessageBody": json.dumps(
                        {
                            "runId": run_id,
                            "phase": phase,
                            "date": date_str,
                            "page": page,
                            "totalPages": effective_total_pages,
                            "taskToken": task_token,
                            "starttime": day_start,
                            "endtime": day_end,
                            "pageSize": page_size,
                            "offset": (page - 1) * page_size + 1,  # USGS offset is 1-based
                        }
                    ),
                }
            )

            if len(batch) == 10:
                _flush_batch(batch)
                dispatched += len(batch)
                batch = []

    if batch:
        _flush_batch(batch)
        dispatched += len(batch)

    logger.info(
        "runId=%s dispatched=%d skippedSuccess=%d skippedBeforePage=%d",
        run_id, dispatched, skipped_success, skipped_before,
    )
    return {
        "runId": run_id,
        "totalPages": effective_total_pages,
        "dispatched": dispatched,
        "skippedSuccess": skipped_success,
        "skippedBeforePage": skipped_before,
    }


# ─────────────────────────────────────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────────────────────────────────────

def _next_run_id(table) -> str:
    """Atomically increment the global run counter and return the new value.

    DynamoDB's ADD expression is atomic — concurrent dispatcher invocations
    each receive a unique, monotonically increasing integer.
    The counter item (pk=COUNTER / sk=RUN_SEQ) is created automatically on
    the first run; no manual seeding is required.
    """
    resp = table.update_item(
        Key={"pk": "COUNTER", "sk": "RUN_SEQ"},
        UpdateExpression="ADD #v :one",
        ExpressionAttributeNames={"#v": "value"},
        ExpressionAttributeValues={":one": 1},
        ReturnValues="UPDATED_NEW",
    )
    return f"{int(resp['Attributes']['value']):06d}"  # e.g. "000001", "000002"


def _load_resume_state(
    table, run_id: str
) -> tuple[str, set[tuple[str, int]], int]:
    """Read the existing META record and the set of already-SUCCESS pages.

    Returns:
        run_id             – unchanged, passed through for clarity
        completed_pages    – set of (date_str, page_number) already SUCCESS
        total_pages_meta   – totalPages stored in the original META record
    """
    meta = table.get_item(Key={"pk": f"RUN#{run_id}", "sk": "META"}).get("Item")
    if not meta:
        raise ValueError(
            f"No META record found for runId={run_id}. "
            "Provide a valid runId or omit it to start a new run."
        )
    total_pages_meta = int(meta.get("totalPages", 0))
    completed_pages = _fetch_completed_pages(table, run_id)
    return run_id, completed_pages, total_pages_meta


def _fetch_completed_pages(table, run_id: str) -> set[tuple[str, int]]:
    """Return (date_str, page_number) pairs for every SUCCESS checkpoint."""
    completed: set[tuple[str, int]] = set()
    last_key = None

    while True:
        kwargs: dict = {
            "KeyConditionExpression": (
                Key("pk").eq(f"RUN#{run_id}") & Key("sk").begins_with("PAGE#")
            ),
            "FilterExpression": Attr("status").eq("SUCCESS"),
            "ProjectionExpression": "sk",
        }
        if last_key:
            kwargs["ExclusiveStartKey"] = last_key

        resp = table.query(**kwargs)

        for item in resp.get("Items", []):
            # sk format: PAGE#fetch#YYYY-MM-DD#00001
            parts = item["sk"].split("#")
            if len(parts) == 4:
                completed.add((parts[2], int(parts[3])))

        last_key = resp.get("LastEvaluatedKey")
        if not last_key:
            break

    return completed


def _generate_day_ranges(starttime: str, endtime: str) -> list[tuple[str, str]]:
    """Expand [starttime, endtime] into a list of (day_start, day_end) tuples."""
    start_date = datetime.strptime(starttime[:10], "%Y-%m-%d").date()
    end_date = datetime.strptime(endtime[:10], "%Y-%m-%d").date()
    ranges: list[tuple[str, str]] = []
    current = start_date
    while current <= end_date:
        ranges.append((f"{current}T00:00:00", f"{current}T23:59:59"))
        current += timedelta(days=1)
    return ranges


def _get_day_count(day_start: str, day_end: str) -> int:
    """/count returns a plain integer – no feature data transferred."""
    resp = requests.get(
        USGS_COUNT_URL,
        params={"starttime": day_start, "endtime": day_end},
        timeout=30,
    )
    if resp.status_code != 200:
        raise RuntimeError(
            f"USGS count failed HTTP {resp.status_code}: {resp.text[:300]}"
        )
    return int(resp.text.strip())


def _resolve_window(inp: dict) -> tuple[str, str]:
    starttime = inp.get("starttime") or ""
    endtime = inp.get("endtime") or ""
    if starttime and endtime:
        return starttime, endtime
    # Default: first day of current UTC month → now
    now = datetime.now(timezone.utc)
    return now.strftime("%Y-%m-01T00:00:00"), now.strftime("%Y-%m-%dT%H:%M:%S")


def _flush_batch(batch: list) -> None:
    resp = sqs.send_message_batch(QueueUrl=SQS_QUEUE_URL, Entries=batch)
    failed = resp.get("Failed", [])
    if failed:
        raise RuntimeError(f"SQS batch send partial failure: {json.dumps(failed)}")
