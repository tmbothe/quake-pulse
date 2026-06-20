import json
import logging
import os

import boto3
from boto3.dynamodb.conditions import Key

logger = logging.getLogger()
logger.setLevel(logging.INFO)

DYNAMODB_TABLE = os.environ["DYNAMODB_TABLE"]

dynamodb = boto3.resource("dynamodb")
sfn = boto3.client("stepfunctions")

TERMINAL_STATES = {"SUCCESS", "FAILED"}


def lambda_handler(event: dict, context) -> None:
    table = dynamodb.Table(DYNAMODB_TABLE)
    evaluated: set[str] = set()

    for record in event["Records"]:
        if record["eventName"] not in ("INSERT", "MODIFY"):
            continue

        new_image = record["dynamodb"].get("NewImage", {})
        sk: str = new_image.get("sk", {}).get("S", "")
        status: str = new_image.get("status", {}).get("S", "")
        run_id: str = new_image.get("runId", {}).get("S", "")

        # Only react to PAGE records reaching a terminal state
        if not sk.startswith("PAGE#") or status not in TERMINAL_STATES:
            continue
        if not run_id or run_id in evaluated:
            continue

        evaluated.add(run_id)
        _evaluate_run(table, run_id)


def _evaluate_run(table, run_id: str) -> None:
    pk = f"RUN#{run_id}"

    # ── Fetch run metadata (total pages + task token) ─────────────────────────
    meta = table.get_item(Key={"pk": pk, "sk": "META"}).get("Item")
    if not meta:
        logger.warning("No META record for runId=%s – skipping", run_id)
        return

    total_pages: int = int(meta["totalPages"])
    task_token: str = meta["taskToken"]

    # ── Count all terminal PAGE items for this run ────────────────────────────
    result = table.query(
        KeyConditionExpression=Key("pk").eq(pk) & Key("sk").begins_with("PAGE#"),
        ProjectionExpression="#s",
        ExpressionAttributeNames={"#s": "status"},
    )
    items = result.get("Items", [])
    terminal_count = sum(1 for i in items if i.get("status") in TERMINAL_STATES)
    failed_items = [i for i in items if i.get("status") == "FAILED"]

    logger.info(
        "runId=%s terminal=%d/%d failed=%d",
        run_id, terminal_count, total_pages, len(failed_items),
    )

    if terminal_count < total_pages:
        return  # Not all pages done yet – wait for next stream event

    # ── Signal Step Functions to release the waitForTaskToken state ───────────
    if not failed_items:
        logger.info("runId=%s – all pages SUCCESS, sending task success", run_id)
        sfn.send_task_success(
            taskToken=task_token,
            output=json.dumps({
                "runId": run_id,
                "totalPages": total_pages,
                "status": "SUCCESS",
            }),
        )
    else:
        logger.error(
            "runId=%s – %d page(s) FAILED, sending task failure",
            run_id, len(failed_items),
        )
        sfn.send_task_failure(
            taskToken=task_token,
            error="RunFailed",
            cause=json.dumps({
                "runId": run_id,
                "totalPages": total_pages,
                "failedCount": len(failed_items),
            }),
        )
