# tests/e2e/test_03_ingest_resilience.py -- Area: Ingest resilience (section 11)
import json
import time

from _helpers import SoftChecks, load_fixture, now_iso


def test_ingest_503_path(aws_clients, stack, valid_token, invoke_lambda):
    """FR-STR-02: override EVENT_STREAM_NAME to a nonexistent stream -> 503
    STREAM_UNAVAILABLE. The try/finally guarantees the env var is restored
    even if the assertions fail."""
    checks = SoftChecks()
    lam = aws_clients["lambda"]

    login_evt = load_fixture("valid", "user.login.json")
    login_evt["clientTimestamp"] = now_iso()

    payload = {
        "body": json.dumps(login_evt),
        "headers": {"authorization": f"Bearer {valid_token}"},
        "httpMethod": "POST",
        "path": "/v1/events",
        "requestContext": {"authorizer": {"tenantId": "tenant_test", "tenantRole": "sdk_writer"}},
    }

    try:
        lam.update_function_configuration(
            FunctionName=stack["ingest_fn"],
            Environment={"Variables": {"EVENT_STREAM_NAME": "nonexistent-stream-xyz", "AWS_REGION": "eu-west-1"}},
        )
        time.sleep(3)

        _, resp_503 = invoke_lambda(stack["ingest_fn"], payload)
        body_503 = json.loads(resp_503["body"])

        checks.check(resp_503["statusCode"] == 503, "Ingest 503: statusCode = 503",
                     f"expected 503, got {resp_503['statusCode']}")
        checks.check(body_503.get("error") == "STREAM_UNAVAILABLE", "Ingest 503: error = STREAM_UNAVAILABLE",
                     f"expected STREAM_UNAVAILABLE, got '{body_503.get('error')}'")
    finally:
        lam.update_function_configuration(
            FunctionName=stack["ingest_fn"],
            Environment={"Variables": {"EVENT_STREAM_NAME": stack["stream_name"], "AWS_REGION": "eu-west-1"}},
        )
        time.sleep(3)

    # Confirm IngestFunction is back to normal with a real request.
    restore_payload = {
        "body": json.dumps(login_evt),
        "headers": {},
        "requestContext": {"authorizer": {"tenantId": "tenant_test", "tenantRole": "sdk_writer"}},
    }
    _, restore_resp = invoke_lambda(stack["ingest_fn"], restore_payload)
    checks.check(restore_resp["statusCode"] == 202, "Ingest restored: 202 accepted after stream name fix",
                 f"expected 202, got {restore_resp['statusCode']}")

    checks.done()
