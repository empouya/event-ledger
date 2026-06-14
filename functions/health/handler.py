import json
import logging
import os
from datetime import datetime, timezone

import boto3
from botocore.exceptions import ClientError

logger = logging.getLogger()
logger.setLevel(logging.INFO)

# describe_table is a metadata call — no data read, no cost, sub-millisecond.
# We use the client (not the resource) because describe_table is a control-plane
# operation, not a data-plane one.
dynamodb = boto3.client("dynamodb", region_name=os.environ.get("AWS_REGION", "eu-west-1"))
TABLE_NAME = os.environ["TABLE_NAME"]


def lambda_handler(event, context):
    """
    GET /health — FR-ING-06.
    Returns 200 {"status": "healthy", ...} when all checks pass,
    or 503 {"status": "degraded", ...} when any check fails.
    """
    timestamp = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"

    # ── DynamoDB connectivity check ────────────────────────────────────────────
    # describe_table succeeds even on an empty table; it only fails if the
    # service is unreachable or the table doesn't exist — both are real problems.
    dynamodb_ok = True
    try:
        dynamodb.describe_table(TableName=TABLE_NAME)
    except (ClientError, Exception) as exc:
        logger.error(json.dumps({
            "message": "DynamoDB health check failed",
            "request_id": context.aws_request_id,
            "error": str(exc),
        }))
        dynamodb_ok = False

    checks = {"dynamodb": "ok" if dynamodb_ok else "unreachable"}
    status  = "healthy"  if dynamodb_ok else "degraded"
    status_code = 200   if dynamodb_ok else 503

    logger.info(json.dumps({
        "message": "health check",
        "request_id": context.aws_request_id,
        "status": status,
        "checks": checks,
    }))

    return {
        "statusCode": status_code,
        "headers": {"Content-Type": "application/json"},
        "body": json.dumps({
            "status":    status,
            "timestamp": timestamp,
            "checks":    checks,
        }),
    }
