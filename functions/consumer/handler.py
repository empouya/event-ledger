import base64
import json
import logging
import os
from datetime import datetime, timezone, timedelta

import boto3

logger = logging.getLogger()
logger.setLevel(logging.INFO)

# boto3.resource gives us a higher-level Table object with nicer put_item syntax.
# boto3.client (used in the Ingest Lambda) is lower-level — both are fine,
# but resource is more ergonomic for DynamoDB table operations.
dynamodb = boto3.resource("dynamodb", region_name=os.environ.get("AWS_REGION", "eu-west-1"))
TABLE_NAME = os.environ["TABLE_NAME"]
table = dynamodb.Table(TABLE_NAME)


def lambda_handler(event, context):
    """
    Kinesis trigger — called with a batch of up to 100 records.
    If this function raises an unhandled exception, Kinesis retries the entire
    batch. BisectBatchOnFunctionError (set in template.yaml) will then split
    the batch in half to isolate the bad record.
    """
    records = event.get("Records", [])

    logger.info(json.dumps({
        "message": "consumer batch received",
        "request_id": context.aws_request_id,
        "record_count": len(records),
    }))

    for record in records:
        # Kinesis delivers data as a base64-encoded string.
        # Decode bytes → UTF-8 string → parse as JSON to get the original payload
        # that the Ingest Lambda wrote with json.dumps(...).encode("utf-8").
        raw = base64.b64decode(record["kinesis"]["data"]).decode("utf-8")
        payload = json.loads(raw)
        _process_record(payload, context)

    logger.info(json.dumps({
        "message": "consumer batch complete",
        "request_id": context.aws_request_id,
        "processed": len(records),
    }))


def _process_record(payload: dict, context) -> None:
    """
    Enriches one event and writes it to DynamoDB.
    Any exception propagates up to lambda_handler, causing Kinesis to retry.
    """
    tenant_id    = payload.get("tenantId",    "unknown")
    event_type   = payload.get("eventType",   "unknown")
    ingestion_id = payload.get("ingestionId", "unknown")

    # processedAt = right now, in the Consumer Lambda.
    # Distinct from ingestedAt (when the API accepted it) — the difference
    # measures end-to-end pipeline latency (FR-PROC-02).
    processed_at = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"

    # expiresAt must be a Unix epoch integer (seconds since 1970-01-01).
    # DynamoDB's TTL daemon scans for items where expiresAt < now() and deletes them.
    # A string or float here will make TTL silently ignore the attribute.
    expires_at = int((datetime.now(timezone.utc) + timedelta(days=90)).timestamp())

    # Build the full DynamoDB item.
    # **payload carries every field the Ingest Lambda wrote (eventType, tenantId,
    # ingestionId, ingestedAt, plus any caller-supplied fields like orderId).
    # We then overwrite/add the keys and processing metadata on top.
    item = {
        **payload,
        # ── Primary table keys ──────────────────────────────────────────────
        "PK":     f"{tenant_id}#{event_type}",   # one partition per tenant+type
        "SK":     f"{processed_at}#{ingestion_id}",  # chronological + unique
        # ── GSI-1 keys ──────────────────────────────────────────────────────
        "GSI1PK": tenant_id,       # query all types for a tenant
        "GSI1SK": processed_at,    # time-ordered within that tenant
        # ── Processing metadata ──────────────────────────────────────────────
        "processedAt": processed_at,
        "expiresAt":   expires_at,   # integer epoch — DynamoDB TTL requirement
        "status":      "processed",
    }

    logger.info(json.dumps({
        "message": "writing event to DynamoDB",
        "request_id": context.aws_request_id,
        "tenant_id": tenant_id,
        "event_type": event_type,
        "ingestion_id": ingestion_id,
        "pk": item["PK"],
        "sk": item["SK"],
    }))

    # put_item overwrites any existing item with the same PK+SK.
    # Because SK includes ingestionId (a UUID), collisions are practically impossible.
    table.put_item(Item=item)
