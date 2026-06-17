import json
import logging
import os
from datetime import datetime, timezone, timedelta

import boto3
from botocore.exceptions import ClientError

logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Module-scope clients — reused across warm invocations.
dynamodb = boto3.client("dynamodb", region_name=os.environ.get("AWS_REGION", "eu-west-1"))
s3 = boto3.client("s3", region_name=os.environ.get("AWS_REGION", "eu-west-1"))

TABLE_NAME   = os.environ["TABLE_NAME"]
EVENTS_BUCKET = os.environ["EVENTS_BUCKET"]

# Events expire from DynamoDB after 90 days. DynamoDB TTL deletes items
# asynchronously — the exact deletion time is not guaranteed to the second.
TTL_DAYS = 90


def lambda_handler(event: dict, context) -> dict:
    """
    Receives the enriched, PII-scrubbed event envelope from Step Functions.
    Writes it to DynamoDB and S3, then returns the event unchanged so the
    state machine can record a clean final status.

    DynamoDB write is idempotent via ConditionExpression: if the same
    PK+SK already exists (retry or duplicate delivery), the write is
    silently skipped. S3 PutObject is naturally idempotent — writing the
    same key twice just overwrites with the same content.
    """
    tenant_id        = event.get("tenantId", "")
    event_id         = event.get("eventId", "")
    event_type       = event.get("eventType", "")
    ingested_at      = event.get("ingestedAt", "")
    processed_at     = event.get("processedAt", "")
    ingestion_id     = event.get("ingestionId", "")
    schema_version   = event.get("schemaVersionUsed", event.get("schemaVersion", ""))
    pipeline_version = event.get("pipelineVersion", "")
    payload          = event.get("payload", {})

    logger.info(json.dumps({
        "message": "writer invoked",
        "request_id": context.aws_request_id,
        "event_id": event_id,
        "tenant_id": tenant_id,
        "event_type": event_type,
    }))

    # ── Key design ────────────────────────────────────────────────────────────
    # PK = tenantId#eventType — collocates all events of a type per tenant
    #                           on one partition for fast range queries.
    # SK = ingestedAt#eventId — stable across retries (unlike processedAt);
    #                           guarantees uniqueness because eventId is UUID v4.
    pk = f"{tenant_id}#{event_type}"
    sk = f"{ingested_at}#{event_id}"

    try:
        ingested_dt = datetime.fromisoformat(ingested_at.replace("Z", "+00:00"))
    except (ValueError, AttributeError):
        ingested_dt = datetime.now(timezone.utc)
    expires_at = int((ingested_dt + timedelta(days=TTL_DAYS)).timestamp())

    # ── DynamoDB write ────────────────────────────────────────────────────────
    try:
        dynamodb.put_item(
            TableName=TABLE_NAME,
            Item={
                "PK":                {"S": pk},
                "SK":                {"S": sk},
                "GSI1PK":            {"S": tenant_id},
                "GSI1SK":            {"S": ingested_at},
                "eventId":           {"S": event_id},
                "eventType":         {"S": event_type},
                "tenantId":          {"S": tenant_id},
                "ingestionId":       {"S": ingestion_id},
                "ingestedAt":        {"S": ingested_at},
                "processedAt":       {"S": processed_at},
                "pipelineVersion":   {"S": pipeline_version},
                "schemaVersionUsed": {"S": schema_version},
                "payload":           {"S": json.dumps(payload)},
                "status":            {"S": "processed"},
                "expiresAt":         {"N": str(expires_at)},
            },
            ConditionExpression="attribute_not_exists(PK)",
        )
        logger.info(json.dumps({
            "message": "event written to dynamodb",
            "request_id": context.aws_request_id,
            "event_id": event_id,
            "pk": pk,
            "sk": sk,
        }))
    except ClientError as e:
        if e.response["Error"]["Code"] == "ConditionalCheckFailedException":
            # Item with this PK+SK already exists — this is a retry or duplicate
            # delivery. Silently skip the write; the existing item is correct.
            logger.info(json.dumps({
                "message": "duplicate write suppressed",
                "request_id": context.aws_request_id,
                "event_id": event_id,
                "pk": pk,
                "sk": sk,
            }))
        else:
            raise

    # ── S3 write ──────────────────────────────────────────────────────────────
    # S3 PutObject is naturally idempotent: writing the same key twice
    # overwrites with identical content — no duplicates are possible.
    # Date partitioning (YYYY-MM-DD) enables Athena to prune to a single
    # day's prefix rather than scanning the full bucket.
    date_prefix = ingested_at[:10] if ingested_at else "unknown"
    s3_key = f"{tenant_id}/{event_type}/{date_prefix}/{event_id}.json"

    s3.put_object(
        Bucket=EVENTS_BUCKET,
        Key=s3_key,
        Body=json.dumps(event).encode("utf-8"),
        ContentType="application/json",
    )

    logger.info(json.dumps({
        "message": "event written to s3",
        "request_id": context.aws_request_id,
        "event_id": event_id,
        "s3_key": s3_key,
    }))

    return event
