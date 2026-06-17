import json
import logging
import uuid
from datetime import datetime, timezone, timedelta

import boto3

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

def _reject(failure_code: str, detail: str, event_id: str, event_type: str) -> None:
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

    Note: tenant-specific event type allow-listing (requires the
    TenantConfiguration table) is deferred and will be added in a later
    increment once that table exists.
    """
    event_id   = event.get("eventId",   "")
    event_type = event.get("eventType", "")

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
        )

    # ── eventId: must be UUID v4 ─────────────────────────────────────────────
    if not event_id or not _is_uuid4(event_id):
        _reject(
            "INVALID_EVENT_ID",
            f"eventId must be a UUID v4; received: '{event_id}'",
            event_id,
            event_type,
        )

    # ── eventType: must be a registered type ─────────────────────────────────
    if event_type not in REGISTERED_EVENT_TYPES:
        _reject(
            "UNKNOWN_EVENT_TYPE",
            f"eventType '{event_type}' is not registered",
            event_id,
            event_type,
        )

    # ── schemaVersion: must match the current version ────────────────────────
    schema_version = event.get("schemaVersion", "")
    if schema_version != CURRENT_SCHEMA_VERSION:
        _reject(
            "SCHEMA_VERSION_MISMATCH",
            f"schemaVersion must be '{CURRENT_SCHEMA_VERSION}'; received: '{schema_version}'",
            event_id,
            event_type,
        )

    # ── clientTimestamp: must be a valid ISO 8601 UTC string ─────────────────
    client_ts_str = event.get("clientTimestamp", "")
    if not _is_valid_iso8601_utc(client_ts_str):
        _reject(
            "INVALID_TIMESTAMP",
            f"clientTimestamp is not a valid ISO 8601 UTC timestamp",
            event_id,
            event_type,
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
        )

    # ── clientTimestamp: must not be more than 5 minutes in the future ───────
    # Clocks on client devices can drift; 5 minutes is a reasonable tolerance.
    if client_ts > now + timedelta(minutes=5):
        _reject(
            "TIMESTAMP_IN_FUTURE",
            "clientTimestamp is more than 5 minutes in the future",
            event_id,
            event_type,
        )

    # ── payload: must be present and a JSON object ───────────────────────────
    payload = event.get("payload")
    if payload is None or not isinstance(payload, dict):
        _reject(
            "INVALID_PAYLOAD",
            "payload must be a non-null JSON object",
            event_id,
            event_type,
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
