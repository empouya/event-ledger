# tests/e2e/conftest.py
#
# Session fixtures for the LocalStack e2e suite: AWS clients, stack resource
# lookups, JWT minting, and Lambda invocation. This replaces the PowerShell
# _common.ps1 script -- one-time lookups now live in session-scoped fixtures
# instead of being re-run at the top of every dot-sourced file.
import json
import os
import time

import boto3
import jwt
import pytest
import requests

STACK_NAME = "streamcore-local"
REGION = "eu-west-1"
LOCALSTACK_ENDPOINT = os.environ.get("LOCALSTACK_ENDPOINT", "http://localhost:4566")
JWT_SECRET_LOCAL = os.environ.get(
    "JWT_SECRET_LOCAL", "streamcore-local-dev-jwt-secret-CHANGE-FOR-REAL-AWS"
)

_SERVICES = [
    "cloudformation", "lambda", "kinesis", "dynamodb", "s3", "sqs", "sns",
    "stepfunctions", "apigateway", "kms", "secretsmanager",
]


@pytest.fixture(scope="session")
def aws_clients():
    return {
        name: boto3.client(
            name,
            endpoint_url=LOCALSTACK_ENDPOINT,
            region_name=REGION,
            aws_access_key_id="test",
            aws_secret_access_key="test",
        )
        for name in _SERVICES
    }


@pytest.fixture(scope="session")
def stack(aws_clients):
    """Physical resource ids + stack outputs, resolved once per test session."""
    cfn = aws_clients["cloudformation"]

    def physical_id(logical_id):
        return cfn.describe_stack_resource(
            StackName=STACK_NAME, LogicalResourceId=logical_id
        )["StackResourceDetail"]["PhysicalResourceId"]

    outputs = {
        o["OutputKey"]: o["OutputValue"]
        for o in cfn.describe_stacks(StackName=STACK_NAME)["Stacks"][0].get("Outputs", [])
    }

    info = {
        "consumer_fn": physical_id("ConsumerFunction"),
        "writer_fn": physical_id("WriterFunction"),
        "ingest_fn": physical_id("IngestFunction"),
        "authorizer_fn": physical_id("AuthorizerFunction"),
        "summary_fn": physical_id("SummaryFunction"),
        "stream_name": physical_id("EventStream"),
        "sm_arn": outputs["ProcessingStateMachineArn"],
        "validation_dlq_url": outputs["ValidationDLQUrl"],
        "daily_reports_topic_arn": outputs.get("DailyReportsTopicArn"),
        "outputs": outputs,
    }
    print("\n" + "\n".join(f"{k}: {v}" for k, v in info.items() if k != "outputs"))
    return info


@pytest.fixture(scope="session")
def base_url(aws_clients):
    override = os.environ.get("LOCAL_BASE_URL")
    if override:
        return override
    api_id = aws_clients["apigateway"].get_rest_apis()["items"][0]["id"]
    return f"{LOCALSTACK_ENDPOINT}/restapis/{api_id}/dev/_user_request_"


def _mint_jwt(tenant_id="tenant_test", tenant_role="sdk_writer", expires_in=3600):
    now = int(time.time())
    payload = {
        "iss": "streamcore-local",
        "sub": tenant_id,
        "tenantId": tenant_id,
        "tenantRole": tenant_role,
        "iat": now,
        "exp": now + expires_in,
    }
    return jwt.encode(payload, JWT_SECRET_LOCAL, algorithm="HS256")


@pytest.fixture(scope="session")
def make_jwt():
    """Factory: make_jwt(tenant_id=..., tenant_role=..., expires_in=...) -> token str."""
    return _mint_jwt


@pytest.fixture(scope="session")
def valid_token(make_jwt):
    return make_jwt()


@pytest.fixture
def invoke_lambda(aws_clients):
    """invoke_lambda(function_name, payload_dict) -> (raw_boto3_response, parsed_body)."""
    def _invoke(function_name, payload):
        resp = aws_clients["lambda"].invoke(
            FunctionName=function_name,
            Payload=json.dumps(payload).encode("utf-8"),
        )
        body_raw = resp["Payload"].read()
        body = json.loads(body_raw) if body_raw else None
        return resp, body
    return _invoke


@pytest.fixture
def http_post():
    """http_post(url, headers, body_str) -> status_code. Never raises on non-2xx --
    `requests` only raises on connection-level failures, unlike PowerShell's
    Invoke-WebRequest which throws on any non-2xx and needs an exception-type
    catch that doesn't even match across PowerShell 5.1 vs 7 (see migration notes)."""
    def _post(url, headers, body):
        merged = {"Content-Type": "application/json", **headers}
        r = requests.post(url, headers=merged, data=body, timeout=30)
        return r.status_code
    return _post
