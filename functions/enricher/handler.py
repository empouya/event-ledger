import json
import logging
import os
import time
from datetime import datetime, timezone

from aws_xray_sdk.core import xray_recorder, patch_all
patch_all()

logger = logging.getLogger()
logger.setLevel(logging.INFO)

# PIPELINE_VERSION is injected by the SAM template at deploy time.
# Using an env var (not a hardcoded string) means the version is part of
# the IaC definition and shows up in CloudFormation change sets.
PIPELINE_VERSION = os.environ.get("PIPELINE_VERSION", "2.0.0")


def lambda_handler(event: dict, context) -> dict:
    """
    Receives the PII-scrubbed event envelope from Step Functions.
    Adds three pipeline-side metadata fields and returns the enriched event.
    These fields cannot be set by the client SDK — they are pipeline facts.
    """
    event_id = event.get("eventId", "")

    logger.info(json.dumps({
        "message": "enricher invoked",
        "request_id": context.aws_request_id,
        "event_id": event_id,
    }))

    try:
        xray_recorder.put_annotation("pipelineVersion", PIPELINE_VERSION)
    except Exception:
        pass

    # Capture the processing timestamp once so all three fields are consistent.
    processed_at = (
        datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"
    )

    tenant_id   = event.get("tenantId",  "")
    event_type  = event.get("eventType", "")
    ingested_at = event.get("ingestedAt", "")
    try:
        if ingested_at:
            t_in  = datetime.fromisoformat(ingested_at.replace("Z", "+00:00"))
            t_out = datetime.fromisoformat(processed_at.replace("Z", "+00:00"))
            latency_ms = int((t_out - t_in).total_seconds() * 1000)
            if latency_ms >= 0:
                _emit_processing_latency_metric(tenant_id, event_type, latency_ms)
    except Exception:
        pass

    result = {
        **event,
        "processedAt": processed_at,
        "pipelineVersion": PIPELINE_VERSION,
        "schemaVersionUsed": event.get("schemaVersion", ""),
    }

    logger.info(json.dumps({
        "message": "enrichment complete",
        "request_id": context.aws_request_id,
        "event_id": event_id,
        "processedAt": processed_at,
        "pipelineVersion": PIPELINE_VERSION,
    }))

    return result


def _emit_processing_latency_metric(tenant_id: str, event_type: str, latency_ms: int) -> None:
    emf = {
        "_aws": {
            "Timestamp": int(time.time() * 1000),
            "CloudWatchMetrics": [
                {
                    "Namespace": "StreamCore/Pipeline",
                    "Dimensions": [["TenantId", "EventType"]],
                    "Metrics": [
                        {"Name": "ProcessingLatencyMs", "Unit": "Milliseconds"},
                    ],
                }
            ],
        },
        "TenantId": tenant_id,
        "EventType": event_type,
        "ProcessingLatencyMs": latency_ms,
    }
    print(json.dumps(emf))
