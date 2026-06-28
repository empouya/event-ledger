import base64
import json
import logging
import os

import boto3
from aws_xray_sdk.core import xray_recorder, patch_all
patch_all()

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
    Kinesis trigger — processes a batch of up to 100 records.

    For each record, decodes the payload and starts one Step Functions
    execution. Records that fail are collected and returned in the
    batchItemFailures list so the ESM retries only those records —
    not the whole batch.

    Two safety layers work together:
      1. ReportBatchItemFailures (this return value): lets good records
         checkpoint while bad ones retry. Isolates failures at record level.
      2. BisectBatchOnFunctionError (template.yaml): if this function
         crashes entirely (unhandled exception), Kinesis bisects the batch
         and retries each half — a second line of defence.

    After MaximumRetryAttempts retries, exhausted records go to the
    on-failure SQS destination configured on the ESM.
    """
    records = event.get("Records", [])
    batch_item_failures = []

    try:
        xray_recorder.put_annotation("batchSize", len(records))
    except Exception:
        pass

    logger.info(json.dumps({
        "message": "consumer batch received",
        "request_id": context.aws_request_id,
        "record_count": len(records),
    }))

    for record in records:
        # The sequence number is the ESM's handle for this specific record.
        # We report it back in batchItemFailures if processing fails.
        seq = record["kinesis"]["sequenceNumber"]
        try:
            # Kinesis delivers data as a base64-encoded string.
            raw = base64.b64decode(record["kinesis"]["data"]).decode("utf-8")
            payload = json.loads(raw)
            _start_processing(payload, context)
        except Exception as e:
            # Log the failure without the raw payload (may contain PII).
            logger.error(json.dumps({
                "message": "record processing failed — marking for retry",
                "request_id": context.aws_request_id,
                "sequence_number": seq,
                "error_type": type(e).__name__,
                "error": str(e),
            }))
            batch_item_failures.append({"itemIdentifier": seq})

    dispatched = len(records) - len(batch_item_failures)
    logger.info(json.dumps({
        "message": "consumer batch complete",
        "request_id": context.aws_request_id,
        "dispatched": dispatched,
        "failed": len(batch_item_failures),
    }))

    # Return the partial-batch failure report.
    # An empty list means all records succeeded — Kinesis checkpoints the batch.
    return {"batchItemFailures": batch_item_failures}


def _start_processing(payload: dict, context) -> None:
    """
    Starts one Step Functions execution for the given event payload.

    Execution name = ingestionId (a UUID v4 stamped by the Ingest Lambda).
    This makes the dispatch idempotent: if Kinesis redelivers the same
    record, StartExecution returns ExecutionAlreadyExists and we skip it —
    the state machine is already running or has already completed.
    ExecutionAlreadyExists is NOT a failure — it is never added to
    batchItemFailures.
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
        # Kinesis redelivered this record — execution already running or done.
        logger.info(json.dumps({
            "message": "execution already exists, skipping",
            "request_id": context.aws_request_id,
            "ingestion_id": ingestion_id,
        }))
