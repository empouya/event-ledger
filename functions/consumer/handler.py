import base64
import json
import logging
import os

import boto3

logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Module-scope client — reused across warm invocations.
stepfunctions = boto3.client(
    "stepfunctions",
    region_name=os.environ.get("AWS_REGION", "eu-west-1"),
)

STATE_MACHINE_ARN = os.environ["STATE_MACHINE_ARN"]


def lambda_handler(event, context):
    """
    Kinesis trigger — called with a batch of up to 100 records.
    For each record, starts one Step Functions execution asynchronously.
    The state machine handles all processing; the Consumer's only job
    is to decode and dispatch.

    If this function raises an unhandled exception, Kinesis retries the
    entire batch. BisectBatchOnFunctionError (set in template.yaml) splits
    the batch in half to isolate bad records.
    """
    records = event.get("Records", [])

    logger.info(json.dumps({
        "message": "consumer batch received",
        "request_id": context.aws_request_id,
        "record_count": len(records),
    }))

    for record in records:
        # Kinesis delivers data as a base64-encoded string.
        raw = base64.b64decode(record["kinesis"]["data"]).decode("utf-8")
        payload = json.loads(raw)
        _start_processing(payload, context)

    logger.info(json.dumps({
        "message": "consumer batch dispatched",
        "request_id": context.aws_request_id,
        "dispatched": len(records),
    }))


def _start_processing(payload: dict, context) -> None:
    """
    Starts one Step Functions execution for the given event payload.

    Execution name = ingestionId (a UUID v4 stamped by the Ingest Lambda).
    This makes the dispatch idempotent: if Kinesis redelivers the same
    record, StartExecution returns ExecutionAlreadyExists and we skip it —
    the state machine is already running or has already completed.
    """
    ingestion_id = payload.get("ingestionId", "")
    tenant_id    = payload.get("tenantId",    "")
    event_type   = payload.get("eventType",   "")

    logger.info(json.dumps({
        "message": "dispatching event to state machine",
        "request_id": context.aws_request_id,
        "ingestion_id": ingestion_id,
        "tenant_id": tenant_id,
        "event_type": event_type,
    }))

    try:
        stepfunctions.start_execution(
            stateMachineArn=STATE_MACHINE_ARN,
            name=ingestion_id,
            input=json.dumps(payload),
        )
    except stepfunctions.exceptions.ExecutionAlreadyExists:
        # Kinesis redelivered this record — the execution is already running
        # or has completed. Nothing to do.
        logger.info(json.dumps({
            "message": "execution already exists, skipping",
            "request_id": context.aws_request_id,
            "ingestion_id": ingestion_id,
        }))
