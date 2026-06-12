import json
import logging

# Configure structured logging.
# Using JSON format means CloudWatch can index each field individually,
# making logs searchable and filterable by level, request ID, etc.
logger = logging.getLogger()
logger.setLevel(logging.INFO)


def lambda_handler(event, context):
    """
    Health check endpoint. Returns HTTP 200 with {"status": "ok"}.

    AWS passes two arguments to every Lambda handler:
    - event: the incoming request data (HTTP method, path, headers, body, etc.)
    - context: runtime metadata (function name, remaining time, request ID, etc.)
    """

    logger.info(json.dumps({
        "message": "health check called",
        "request_id": context.aws_request_id,
    }))

    return {
        "statusCode": 200,
        # API Gateway requires the body to be a string, not a dict —
        # that is why json.dumps is used here instead of returning the dict directly.
        "body": json.dumps({"status": "ok"}),
        "headers": {
            "Content-Type": "application/json",
        },
    }
