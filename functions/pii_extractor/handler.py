import hashlib
import hmac
import json
import logging
import os

import boto3

logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Module-scope client — reused across warm invocations to avoid
# re-establishing the connection on every call.
secretsmanager = boto3.client(
    "secretsmanager",
    region_name=os.environ.get("AWS_REGION", "eu-west-1"),
)

# Module-scope salt cache: tenant_id -> bytes.
# Populated on the first invocation for a given tenant; reused on all
# subsequent warm invocations in the same execution environment.
# Salts are never rotated (rotating would change all digests, breaking
# any downstream linkage that relies on stable pseudonyms).
_salt_cache: dict = {}

# Names of payload fields that contain PII and must be pseudonymised.
# Defined here so adding a new PII field is a one-line change.
_PII_FIELDS = {"userId", "userEmail"}


def _get_salt(tenant_id: str) -> bytes:
    """
    Retrieve the per-tenant PII salt from Secrets Manager.
    The salt is a 256-bit random value stored as a 64-char hex string
    under the key 'piiSalt' in the secret JSON.
    """
    if tenant_id in _salt_cache:
        return _salt_cache[tenant_id]

    secret_name = f"streamcore/pii-salt/{tenant_id}"
    response = secretsmanager.get_secret_value(SecretId=secret_name)
    secret = json.loads(response["SecretString"])
    salt_bytes = bytes.fromhex(secret["piiSalt"])
    _salt_cache[tenant_id] = salt_bytes

    logger.info(json.dumps({
        "message": "pii salt loaded",
        "tenant_id": tenant_id,
        "secret_name": secret_name,
    }))

    return salt_bytes


def _hmac_sha256(value: str, salt: bytes) -> str:
    """
    Return the HMAC-SHA256 digest of value keyed by salt.
    Output is a 64-character lowercase hex string.
    Deterministic: same value + same salt always produces the same digest.
    """
    return hmac.new(salt, value.encode("utf-8"), hashlib.sha256).hexdigest()


def lambda_handler(event: dict, context) -> dict:
    """
    Receives the validated event envelope passed by Step Functions.
    Replaces every PII field in event['payload'] with its HMAC-SHA256 digest.
    Returns the modified envelope; Step Functions writes the return value
    back to the execution's current state (ResultPath: '$').

    Raw PII values are never logged, included in any response, or persisted.
    They exist only in local variables that go out of scope before this
    function returns.
    """
    tenant_id = event.get("tenantId", "")
    event_id = event.get("eventId", "")

    logger.info(json.dumps({
        "message": "pii extractor invoked",
        "request_id": context.aws_request_id,
        "event_id": event_id,
        "tenant_id": tenant_id,
    }))

    salt = _get_salt(tenant_id)

    # Build a new payload dict so we never mutate the original.
    payload = {**event.get("payload", {})}
    hashed_count = 0

    for field in _PII_FIELDS:
        if field in payload and payload[field] is not None:
            # Hash — raw value exists only in this expression, never assigned
            # to a named variable, never reaches any log or return value.
            payload[field] = _hmac_sha256(str(payload[field]), salt)
            hashed_count += 1

    result = {**event, "payload": payload}

    logger.info(json.dumps({
        "message": "pii fields hashed",
        "request_id": context.aws_request_id,
        "event_id": event_id,
        "fields_hashed": hashed_count,
    }))

    return result
