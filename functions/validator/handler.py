import json
import logging
import uuid
import os
import time
from datetime import datetime, timezone, timedelta

import boto3

from aws_xray_sdk.core import xray_recorder, patch_all
patch_all()

logger = logging.getLogger()
logger.setLevel(logging.INFO)

# ── Constants ────────────────────────────────────────────────────────────────

REGISTERED_EVENT_TYPES = {
    "product.viewed",
    "product.added_to_cart",
    "cart.updated",
    "cart.abandoned",
    "checkout.started",
    "order.placed",
    "payment.initiated",
    "payment.succeeded",
    "payment.failed",
    "user.registered",
    "user.login",
    "user.logout",
}

CURRENT_SCHEMA_VERSION = "1.0"

# ISO 4217 currency codes accepted by this pipeline.
ACCEPTED_CURRENCIES = {"EUR", "USD", "GBP", "SEK", "NOK", "DKK", "CHF"}

# Allowed values for payment failure codes.
ACCEPTED_FAILURE_CODES = {
    "insufficient_funds",
    "card_declined",
    "expired_card",
    "invalid_card",
    "authentication_failed",
    "fraud_suspected",
    "bank_error",
    "timeout",
    "duplicate_transaction",
    "unknown",
}

# Maximum allowed event size in bytes (envelope + payload combined).
MAX_EVENT_BYTES = 32 * 1024  # 32 KB


# ── DynamoDB (module scope) ──────────────────────────────────────────────────
# boto3 resource is created once per execution environment (cold start) and
# reused across warm invocations — avoids per-invocation connection overhead.
# Region and endpoint are picked up automatically from the Lambda environment;
# on LocalStack, AWS_ENDPOINT_URL routes calls to localhost:4566.
#
# TENANT_CONFIG_TABLE is empty when the env var is absent (e.g., unit tests
# without the table). _get_active_event_types() checks for this and skips
# the DynamoDB call, so the Validator still works without the table.
TENANT_CONFIG_TABLE = os.environ.get("TENANT_CONFIG_TABLE", "")
_dynamodb = boto3.resource("dynamodb")


# ── Custom exception ─────────────────────────────────────────────────────────

class ValidationError(Exception):
    """
    Raised when an event does not conform to the expected schema or business
    rules. The exception message is a JSON string containing a failureCode
    and a human-readable detail.

    Step Functions catches this by the class name "ValidationError" and
    routes the execution to the rejection terminal state. The class name
    is the catch key — the message is carried in the Cause field of the
    Step Functions error object.
    """
    pass


# ── Helpers ──────────────────────────────────────────────────────────────────

def _reject(failure_code: str, detail: str, event_id: str, event_type: str, tenant_id: str = "") -> None:
    """
    Log the rejection reason (safe — no PII in these fields) and raise
    ValidationError so Step Functions routes to EventRejected.
    """
    logger.warning(json.dumps({
        "message": "event rejected",
        "failureCode": failure_code,
        "detail": detail,
        "eventId": event_id,
        "eventType": event_type,
    }))

    try:
        _emit_validation_failure_metric(tenant_id, event_type, failure_code)
    except Exception:
        pass

    raise ValidationError(json.dumps({
        "failureCode": failure_code,
        "detail": detail,
    }))


def _is_uuid4(value: str) -> bool:
    """
    Returns True only if value is a valid UUID v4 string.
    uuid.UUID(value) parses any UUID; .version == 4 confirms it is v4.
    uuid.UUID(..., version=4) is NOT a validator — it silently adjusts bits.
    """
    try:
        return uuid.UUID(value).version == 4
    except (ValueError, AttributeError):
        return False


def _is_valid_iso8601_utc(value: str) -> bool:
    """
    Returns True if value is a valid ISO 8601 UTC timestamp ending in Z.
    Examples of accepted formats:
      2026-06-15T10:00:00Z
      2026-06-15T10:00:00.452Z
    Python's fromisoformat does not accept the Z suffix before 3.11, so
    we normalise Z -> +00:00 before parsing.
    """
    if not isinstance(value, str) or not value.endswith("Z"):
        return False
    try:
        datetime.fromisoformat(value.replace("Z", "+00:00"))
        return True
    except ValueError:
        return False


def _parse_utc(value: str) -> datetime:
    """Parse a validated ISO 8601 UTC string into a timezone-aware datetime."""
    return datetime.fromisoformat(value.replace("Z", "+00:00"))


def _get_active_event_types(tenant_id: str) -> list:
    """
    Returns the activeEventTypes list for the given tenant from TenantConfiguration.
    An empty list means all registered event types are permitted.

    Fails OPEN on any error — a config-table outage must not stop event processing.
    The distinction from SEC-SECRETS-02 (fail-closed on secret errors) is intentional:
    a missing PII salt means we cannot pseudonymise data (a hard security obligation);
    a missing allow-list is a configuration issue that should not block the pipeline.
    """
    if not TENANT_CONFIG_TABLE:
        logger.warning(json.dumps({
            "message": "TENANT_CONFIG_TABLE not set; skipping VAL-003",
        }))
        return []

    try:
        table = _dynamodb.Table(TENANT_CONFIG_TABLE)
        resp = table.get_item(
            Key={"tenantId": tenant_id},
            ProjectionExpression="activeEventTypes",
        )
        item = resp.get("Item", {})
        return list(item.get("activeEventTypes", []))
    except Exception as exc:
        logger.error(json.dumps({
            "message": "TenantConfiguration GetItem failed; skipping VAL-003",
            "tenantId": tenant_id,
            "error": str(exc),
        }))
        return []


# ── Handler ──────────────────────────────────────────────────────────────────

def lambda_handler(event: dict, context) -> dict:
    """
    Receives the full event envelope as the Step Functions task input.
    Validates all fields that can be checked without external dependencies.

    On success: returns the event unchanged — Step Functions passes it to
    the next state (HashPii).

    On failure: raises ValidationError — Step Functions Catch block routes
    to the EventRejected terminal state. The raw event never reaches
    DynamoDB or S3.

    VAL-003 (tenant event-type allow-list) is enforced here via a DynamoDB
    GetItem on TenantConfiguration if activeEventTypes is non-empty.
    """
    event_id   = event.get("eventId",   "")
    event_type = event.get("eventType", "")
    tenant_id  = event.get("tenantId",  "")

    logger.info(json.dumps({
        "message": "validator invoked",
        "request_id": context.aws_request_id,
        "event_id": event_id,
        "event_type": event_type,
    }))

    # ── Total size check (fast-fail before deeper parsing) ───────────────────
    # json.dumps re-serialises so the byte count is accurate.
    raw_bytes = len(json.dumps(event).encode("utf-8"))
    if raw_bytes > MAX_EVENT_BYTES:
        _reject(
            "PAYLOAD_TOO_LARGE",
            f"Event is {raw_bytes} bytes; limit is {MAX_EVENT_BYTES}",
            event_id,
            event_type,
            tenant_id,
        )

    # ── eventId: must be UUID v4 ─────────────────────────────────────────────
    if not event_id or not _is_uuid4(event_id):
        _reject(
            "INVALID_EVENT_ID",
            f"eventId must be a UUID v4; received: '{event_id}'",
            event_id,
            event_type,
            tenant_id,
        )

    # ── eventType: must be a registered type ─────────────────────────────────
    if event_type not in REGISTERED_EVENT_TYPES:
        _reject(
            "UNKNOWN_EVENT_TYPE",
            f"eventType '{event_type}' is not registered",
            event_id,
            event_type,
            tenant_id,
        )

    # ── Tenant's permitted event-type allow-list ────────────────────
    # Only enforced when activeEventTypes is non-empty. An empty list means the
    # tenant may submit any registered event type (permissive default).
    # Runs after VAL-002 so we only check known types — no point filtering
    # an event type that would be rejected as UNKNOWN anyway.
    active_types = _get_active_event_types(tenant_id)
    if active_types and event_type not in active_types:
        _reject(
            "EVENT_TYPE_NOT_PERMITTED",
            f"eventType '{event_type}' is not in the allow-list for tenant '{tenant_id}'",
            event_id,
            event_type,
            tenant_id,
        )

    # ── schemaVersion: must match the current version ────────────────────────
    schema_version = event.get("schemaVersion", "")
    if schema_version != CURRENT_SCHEMA_VERSION:
        _reject(
            "SCHEMA_VERSION_MISMATCH",
            f"schemaVersion must be '{CURRENT_SCHEMA_VERSION}'; received: '{schema_version}'",
            event_id,
            event_type,
            tenant_id,
        )

    # ── clientTimestamp: must be a valid ISO 8601 UTC string ─────────────────
    client_ts_str = event.get("clientTimestamp", "")
    if not _is_valid_iso8601_utc(client_ts_str):
        _reject(
            "INVALID_TIMESTAMP",
            f"clientTimestamp is not a valid ISO 8601 UTC timestamp",
            event_id,
            event_type,
            tenant_id,
        )

    # ── clientTimestamp: must not be more than 24 hours in the past ──────────
    # This guards against replayed or stale events that have already expired.
    client_ts = _parse_utc(client_ts_str)
    now = datetime.now(timezone.utc)
    if client_ts < now - timedelta(hours=24):
        _reject(
            "TIMESTAMP_TOO_OLD",
            "clientTimestamp is more than 24 hours in the past",
            event_id,
            event_type,
            tenant_id,
        )

    # ── clientTimestamp: must not be more than 5 minutes in the future ───────
    # Clocks on client devices can drift; 5 minutes is a reasonable tolerance.
    if client_ts > now + timedelta(minutes=5):
        _reject(
            "TIMESTAMP_IN_FUTURE",
            "clientTimestamp is more than 5 minutes in the future",
            event_id,
            event_type,
            tenant_id,
        )

    # ── payload: must be present and a JSON object ───────────────────────────
    payload = event.get("payload")
    if payload is None or not isinstance(payload, dict):
        _reject(
            "INVALID_PAYLOAD",
            "payload must be a non-null JSON object",
            event_id,
            event_type,
            tenant_id,
        )

    # ── Event-type-specific rules ─────────────────────────────────────────────
    if event_type == "order.placed":
        _validate_order_placed(payload, event_id, event_type)

    elif event_type == "payment.failed":
        _validate_payment_failed(payload, event_id, event_type)

    # ── All checks passed ─────────────────────────────────────────────────────
    logger.info(json.dumps({
        "message": "validation passed",
        "request_id": context.aws_request_id,
        "event_id": event_id,
        "event_type": event_type,
    }))

    # Return the event unchanged. Step Functions places this at ResultPath "$",
    # making it the complete input to the next state.
    return event


def _validate_order_placed(payload: dict, event_id: str, event_type: str) -> None:
    """Applies order.placed-specific validation rules."""

    # amount must be greater than zero.
    amount = payload.get("amount")
    if not isinstance(amount, (int, float)) or amount <= 0:
        _reject(
            "INVALID_AMOUNT",
            f"order.placed amount must be > 0; received: '{amount}'",
            event_id,
            event_type,
        )

    # items array must have at least one entry.
    items = payload.get("items")
    if not isinstance(items, list) or len(items) < 1:
        _reject(
            "EMPTY_ITEMS_ARRAY",
            "order.placed items must be a non-empty array",
            event_id,
            event_type,
        )

    # currency must be one of the accepted ISO 4217 codes.
    currency = payload.get("currency", "")
    if currency not in ACCEPTED_CURRENCIES:
        _reject(
            "INVALID_CURRENCY",
            f"currency '{currency}' is not accepted; allowed: {sorted(ACCEPTED_CURRENCIES)}",
            event_id,
            event_type,
        )


def _validate_payment_failed(payload: dict, event_id: str, event_type: str) -> None:
    """Applies payment.failed-specific validation rules."""

    failure_code = payload.get("failureCode", "")
    if failure_code not in ACCEPTED_FAILURE_CODES:
        _reject(
            "INVALID_FAILURE_CODE",
            f"failureCode '{failure_code}' is not in the allowed list",
            event_id,
            event_type,
        )


def _emit_validation_failure_metric(tenant_id: str, event_type: str, failure_code: str) -> None:
    emf = {
        "_aws": {
            "Timestamp": int(time.time() * 1000),
            "CloudWatchMetrics": [
                {
                    "Namespace": "StreamCore/Pipeline",
                    "Dimensions": [["TenantId", "EventType"]],
                    "Metrics": [
                        {"Name": "ValidationFailures", "Unit": "Count"},
                    ],
                }
            ],
        },
        "TenantId": tenant_id,
        "EventType": event_type,
        "FailureCode": failure_code,
        "ValidationFailures": 1,
    }
    print(json.dumps(emf))
