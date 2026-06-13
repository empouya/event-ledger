import json
import logging
import os
import uuid
from datetime import datetime, timezone

import boto3

# Structured JSON logging — every entry is a parseable JSON object so
# CloudWatch can index individual fields (level, request_id, tenant_id, etc.).
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Boto3 client created at module level, outside the handler.
# Lambda reuses the same execution environment across warm invocations,
# so this avoids re-establishing the connection on every request.
kinesis = boto3.client("kinesis", region_name=os.environ.get("AWS_REGION", "eu-west-1"))

# Stream name injected via environment variable so we never hardcode it.
# The template passes !Ref EventStream here, which gives us the stream name.
STREAM_NAME = os.environ["EVENT_STREAM_NAME"]


def lambda_handler(event, context):
    """
    POST /v1/events — accept a single event envelope, inject ingestion
    metadata, write to Kinesis, return HTTP 202.

    API Gateway passes the HTTP request as a dict:
      event["body"]    — request body as a string (we must parse it)
      event["headers"] — request headers as a dict
    context.aws_request_id — Lambda's unique ID for this invocation
    """
    request_id = context.aws_request_id

    # ── 1. Parse body ─────────────────────────────────────────────────────
    try:
        body = json.loads(event.get("body") or "{}")
    except (json.JSONDecodeError, TypeError):
        logger.warning(json.dumps({
            "message": "request body is not valid JSON",
            "request_id": request_id,
        }))
        return _error(400, "INVALID_JSON", "Request body must be valid JSON.")

    # Minimal presence check: we only require eventType in this phase.
    # Full envelope validation (eventId, schemaVersion, etc.) is Phase 2.
    if not body.get("eventType"):
        logger.warning(json.dumps({
            "message": "missing required field: eventType",
            "request_id": request_id,
        }))
        return _error(400, "MISSING_FIELD", "Field 'eventType' is required.")

    # ── 2. Tenant identification ──────────────────────────────────────────
    # Phase 4 will extract tenantId from a verified JWT claim.
    # For now we read it from X-Tenant-Id header, defaulting to tenant_dev.
    # API Gateway lowercases header names, so we check both casings.
    headers = event.get("headers") or {}
    tenant_id = (
        headers.get("X-Tenant-Id")
        or headers.get("x-tenant-id")
        or "tenant_dev"
    )

    # ── 3. Inject ingestion metadata ──────────────────────────────────────
    # These fields are appended by the pipeline, not supplied by the SDK.
    ingestion_id = str(uuid.uuid4())
    ingested_at = (
        datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"
    )

    # Merge the system-appended fields into the event body.
    # tenantId from the header overrides any tenantId in the body (FR-ING-03).
    record = {
        **body,
        "tenantId": tenant_id,
        "ingestionId": ingestion_id,
        "ingestedAt": ingested_at,
    }

    # Log context only — never log the full payload at INFO (may contain PII).
    logger.info(json.dumps({
        "message": "writing event to stream",
        "request_id": request_id,
        "tenant_id": tenant_id,
        "event_type": body.get("eventType"),
        "ingestion_id": ingestion_id,
    }))

    # ── 4. Write to Kinesis ───────────────────────────────────────────────
    # PartitionKey = tenantId so all events from the same tenant land on
    # the same shard, preserving intra-tenant ordering (FR-STR-01).
    kinesis.put_record(
        StreamName=STREAM_NAME,
        PartitionKey=tenant_id,
        Data=json.dumps(record).encode("utf-8"),
    )

    logger.info(json.dumps({
        "message": "event accepted",
        "request_id": request_id,
        "ingestion_id": ingestion_id,
    }))

    # ── 5. Return 202 Accepted ────────────────────────────────────────────
    # FR-ING-05 response shape.
    return {
        "statusCode": 202,
        "headers": {"Content-Type": "application/json"},
        "body": json.dumps({
            "status": "accepted",
            "accepted": 1,
            "ingestionId": ingestion_id,
            "timestamp": ingested_at,
        }),
    }


def _error(status_code, code, message):
    """Return a consistent structured error response."""
    return {
        "statusCode": status_code,
        "headers": {"Content-Type": "application/json"},
        "body": json.dumps({"error": code, "message": message}),
    }
