"""
functions/authorizer/handler.py

Mock JWT Lambda REQUEST authorizer -- LocalStack stand-in for the Cognito
User Pool authorizer required by SEC-AUTH-01/02. See ADR-009.

The claims shape (tenantId, tenantRole) is identical to what a real
Cognito authorizer would produce, so IngestFunction needs no changes
when the real Cognito authorizer replaces this mock.

Validation rules (pen-test checklist SS9):
  - Authorization: Bearer header present        -> 401 if missing
  - HS256 signature valid against local secret  -> 401 if invalid
  - Token not expired (exp claim)               -> 401 if expired
  - tenantId claim present and non-empty        -> 403 if missing
  - tenantRole == "sdk_writer"                  -> 403 if wrong role

On success: returns IAM Allow policy + context {tenantId, tenantRole}.
401 path: raises Exception("Unauthorized") -- API GW maps this to HTTP 401.
403 path: returns IAM Deny policy -- API GW maps this to HTTP 403.
"""
import base64
import hashlib
import hmac
import json
import logging
import os
import time

import boto3
from botocore.exceptions import ClientError

from aws_xray_sdk.core import xray_recorder, patch_all
patch_all()

logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Secrets Manager client at module scope -- warm-reused across invocations.
secretsmanager = boto3.client(
    "secretsmanager",
    region_name=os.environ.get("AWS_REGION", "eu-west-1"),
)

# Module-scope secret cache.  Stores (secret_bytes, fetched_at_epoch).
# Refreshed after _SECRET_TTL_S seconds so key rotations are picked up.
_SECRET_CACHE = None
_SECRET_TTL_S = 300   # 5 minutes (SEC-SECRETS-02)
_SECRET_NAME  = "streamcore/jwt-secret"


def _get_jwt_secret() -> bytes:
    """
    Return the HS256 signing secret as bytes, using a 5-minute TTL cache.
    Fails closed: raises on any Secrets Manager error so the invocation
    errors rather than proceeding with a null secret (SEC-SECRETS-02).
    """
    global _SECRET_CACHE
    now = time.time()
    if _SECRET_CACHE and (now - _SECRET_CACHE[1]) < _SECRET_TTL_S:
        return _SECRET_CACHE[0]

    try:
        resp = secretsmanager.get_secret_value(SecretId=_SECRET_NAME)
    except ClientError as exc:
        error_code = exc.response["Error"]["Code"]
        logger.error(json.dumps({
            "message": "authorizer: failed to fetch JWT secret",
            "error_code": error_code,
        }))
        raise  # fail closed -- caller maps to 401

    secret_val   = json.loads(resp["SecretString"])["jwtSecret"]
    secret_bytes = secret_val.encode("utf-8")
    _SECRET_CACHE = (secret_bytes, now)
    return secret_bytes


def _b64url_decode(s: str) -> bytes:
    """Base64url decode (RFC 4648 section 5) with standard padding restoration."""
    s = s.replace("-", "+").replace("_", "/")
    pad = 4 - len(s) % 4
    if pad != 4:
        s += "=" * pad
    return base64.b64decode(s)


def _verify_hs256_jwt(token: str, secret: bytes) -> dict:
    """
    Decode and cryptographically validate an HS256 JWT.
    Returns the claims dict on success.
    Raises ValueError on any structural or cryptographic failure.
    Does NOT raise on authz failures (missing/wrong claims) -- caller handles those.
    """
    parts = token.split(".")
    if len(parts) != 3:
        raise ValueError("malformed JWT: expected 3 dot-separated parts")

    header_b64, payload_b64, sig_b64 = parts

    # 1. Verify signature first -- fail fast before decoding claims.
    #    hmac.compare_digest is constant-time to prevent timing attacks.
    signing_input = f"{header_b64}.{payload_b64}".encode("utf-8")
    expected_sig  = hmac.new(secret, signing_input, hashlib.sha256).digest()
    try:
        actual_sig = _b64url_decode(sig_b64)
    except Exception:
        raise ValueError("malformed JWT signature segment")

    if not hmac.compare_digest(expected_sig, actual_sig):
        raise ValueError("invalid JWT signature")

    # 2. Validate the header algorithm.
    header = json.loads(_b64url_decode(header_b64))
    if header.get("alg") != "HS256":
        raise ValueError(f"unsupported algorithm: {header.get('alg')}")

    # 3. Decode payload and check expiry.
    claims = json.loads(_b64url_decode(payload_b64))
    if time.time() > claims.get("exp", 0):
        raise ValueError("token expired")

    return claims


def _make_policy(effect: str, method_arn: str, context: dict = None) -> dict:
    """Build the IAM policy document returned to API Gateway."""
    policy = {
        "principalId": "tenant",
        "policyDocument": {
            "Version": "2012-10-17",
            "Statement": [{
                "Effect": effect,
                "Action": "execute-api:Invoke",
                "Resource": method_arn,
            }],
        },
    }
    if context:
        policy["context"] = context
    return policy


def lambda_handler(event: dict, context) -> dict:
    """
    API Gateway REQUEST authorizer entry point.

    API Gateway passes the full request as 'event', including headers
    (lowercased), path, method, and requestContext.  We extract the
    Authorization header and validate the Bearer token.

    Return value shapes:
      Allow  -- {"principalId": ..., "policyDocument": {Allow}, "context": {claims}}
      Deny   -- {"principalId": ..., "policyDocument": {Deny}}
      401    -- raise Exception("Unauthorized")
    """
    method_arn = event.get("methodArn", "*")
    headers    = event.get("headers") or {}

    # ---- 1. Extract Bearer token ------------------------------------------
    # API Gateway lowercases header names before passing them to the authorizer.
    auth_header = headers.get("authorization") or headers.get("Authorization") or ""
    if not auth_header.startswith("Bearer "):
        logger.warning(json.dumps({
            "message": "authorizer: missing or malformed Authorization header -> 401",
            "method_arn": method_arn,
        }))
        raise Exception("Unauthorized")

    token = auth_header[len("Bearer "):]

    # ---- 2. Validate the JWT -----------------------------------------------
    try:
        secret = _get_jwt_secret()
        claims = _verify_hs256_jwt(token, secret)
    except ValueError as exc:
        logger.warning(json.dumps({
            "message": "authorizer: JWT validation failed -> 401",
            "reason": str(exc),
            "method_arn": method_arn,
        }))
        raise Exception("Unauthorized")
    except Exception as exc:
        logger.error(json.dumps({
            "message": "authorizer: unexpected error -> 401",
            "error": str(exc),
        }))
        raise Exception("Unauthorized")

    # ---- 3. Check required claims (authz) ----------------------------------
    tenant_id   = claims.get("tenantId", "")
    tenant_role = claims.get("tenantRole", "")

    if not tenant_id:
        logger.warning(json.dumps({
            "message": "authorizer: tenantId claim missing -> Deny (403)",
            "method_arn": method_arn,
        }))
        return _make_policy("Deny", method_arn)

    if tenant_role != "sdk_writer":
        logger.warning(json.dumps({
            "message": "authorizer: tenantRole not sdk_writer -> Deny (403)",
            "tenant_id": tenant_id,
            "tenant_role": tenant_role,
            "method_arn": method_arn,
        }))
        return _make_policy("Deny", method_arn)

    # ---- 4. Allow ----------------------------------------------------------
    logger.info(json.dumps({
        "message": "authorizer: token valid -> Allow",
        "tenant_id": tenant_id,
        "tenant_role": tenant_role,
    }))
    return _make_policy(
        "Allow",
        method_arn,
        context={"tenantId": tenant_id, "tenantRole": tenant_role},
    )
