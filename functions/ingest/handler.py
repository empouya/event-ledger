import json
import logging
import os
import time
import uuid
from datetime import datetime, timezone

import boto3
from botocore.exceptions import ClientError

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

# Kinesis back-off config (FR-STR-02).
# 3 attempts total; delays of 1s and 2s between them.
_MAX_KINESIS_ATTEMPTS = 3
_KINESIS_BASE_DELAY_S = 1


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

    # ── 2. Tenant identification (FR-ING-03) ─────────────────────────────
    # tenantId is extracted from the verified JWT claim that the Lambda
    # authorizer injects into requestContext.authorizer before this
    # function is invoked.  The X-Tenant-Id header stand-in is retired
    # (ADR-004 superseded by ADR-009).
    authorizer_ctx = (event.get("requestContext") or {}).get("authorizer") or {}
    tenant_id = authorizer_ctx.get("tenantId", "")

    # FR-ING-03: if the request body also carries a tenantId that differs
    # from the verified claim, log a warning -- the claim always wins.
    body_tenant = body.get("tenantId", "")
    if body_tenant and body_tenant != tenant_id:
        logger.warning(json.dumps({
            "message": "tenantId in body differs from JWT claim -- using claim",
            "request_id": request_id,
            "claim_tenant_id": tenant_id,
            "body_tenant_id": body_tenant,
        }))

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

    # ── 4. Write to Kinesis (with retry) ──────────────────────────────────
    # PartitionKey = tenantId so all events from the same tenant land on
    # the same shard, preserving intra-tenant ordering (FR-STR-01).
    #
    # FR-STR-02: retry up to _MAX_KINESIS_ATTEMPTS times with exponential
    # back-off before returning 503. Transient throttling (Provisioned-
    # ThroughputExceededException) and short service blips are absorbed
    # here so the caller does not need to handle them.
    write_ok = _put_record_with_retry(
        stream_name=STREAM_NAME,
        partition_key=tenant_id,
        data=json.dumps(record).encode("utf-8"),
        request_id=request_id,
        tenant_id=tenant_id,
    )

    if not write_ok:
        return _error(503, "STREAM_UNAVAILABLE",
                      "Event stream temporarily unavailable. Retry with exponential back-off.")

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


def _put_record_with_retry(
    stream_name: str,
    partition_key: str,
    data: bytes,
    request_id: str,
    tenant_id: str,
) -> bool:
    """
    Attempt kinesis.put_record up to _MAX_KINESIS_ATTEMPTS times.

    Delays between attempts: _KINESIS_BASE_DELAY_S × 2^attempt_index
      attempt 0 → immediate
      attempt 1 → sleep 1 s
      attempt 2 → sleep 2 s

    Returns True on success, False if all attempts fail.
    On final failure emits a KinesisWriteFailure EMF metric.
    """
    last_exc = None

    for attempt in range(_MAX_KINESIS_ATTEMPTS):
        if attempt > 0:
            delay = _KINESIS_BASE_DELAY_S * (2 ** (attempt - 1))
            logger.warning(json.dumps({
                "message": "kinesis put_record failed, retrying",
                "request_id": request_id,
                "tenant_id": tenant_id,
                "attempt": attempt,
                "retry_delay_s": delay,
                "error_type": type(last_exc).__name__,
                "error": str(last_exc),
            }))
            time.sleep(delay)

        try:
            kinesis.put_record(
                StreamName=stream_name,
                PartitionKey=partition_key,
                Data=data,
            )
            return True
        except Exception as exc:  # noqa: BLE001
            last_exc = exc

    # All attempts exhausted.
    logger.error(json.dumps({
        "message": "kinesis put_record failed after all retries, returning 503",
        "request_id": request_id,
        "tenant_id": tenant_id,
        "attempts": _MAX_KINESIS_ATTEMPTS,
        "error_type": type(last_exc).__name__,
        "error": str(last_exc),
    }))

    # FR-STR-02: emit KinesisWriteFailure EMF metric so we can alarm on
    # sustained write failures per tenant. CloudWatch Logs extracts this
    # automatically — no additional IAM or infrastructure required.
    _emit_kinesis_write_failure_metric(tenant_id)

    return False


def _emit_kinesis_write_failure_metric(tenant_id: str) -> None:
    """
    Emit one KinesisWriteFailure count metric via Embedded Metrics Format.

    CloudWatch Logs scans each log line for the _aws.CloudWatchMetrics
    structure and extracts declared metrics automatically. This is zero-
    overhead compared to cloudwatch:PutMetricData and requires no extra
    permissions — Lambda already has logs:PutLogEvents.
    """
    emf = {
        "_aws": {
            "Timestamp": int(time.time() * 1000),
            "CloudWatchMetrics": [
                {
                    "Namespace": "StreamCore/Ingest",
                    "Dimensions": [["TenantId"]],
                    "Metrics": [
                        {"Name": "KinesisWriteFailure", "Unit": "Count"},
                    ],
                }
            ],
        },
        "TenantId": tenant_id,
        "KinesisWriteFailure": 1,
    }
    # print (not logger) so the line reaches CloudWatch Logs unmodified —
    # the logger adds a timestamp prefix that breaks EMF parsing.
    print(json.dumps(emf))


def _error(status_code, code, message):
    """Return a consistent structured error response."""
    return {
        "statusCode": status_code,
        "headers": {"Content-Type": "application/json"},
        "body": json.dumps({"error": code, "message": message}),
    }
