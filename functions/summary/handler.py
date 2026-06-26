import json
import logging
import os
from collections import Counter
from datetime import datetime, date, timedelta, timezone

import boto3
from boto3.dynamodb.conditions import Key

logger = logging.getLogger()
logger.setLevel(logging.INFO)

# ── Module-scope resources (created once per execution environment) ────────────
dynamodb         = boto3.resource("dynamodb")
EVENTS_TABLE     = os.environ["EVENTS_TABLE"]
EVENTS_GSI       = os.environ.get("EVENTS_GSI", "GSI1")
TENANT_TABLE     = os.environ["TENANT_CONFIG_TABLE"]


# ── Entry point ───────────────────────────────────────────────────────────────

def lambda_handler(event: dict, context) -> dict:
    """
    Aggregates the previous calendar day's events per active tenant.

    Input (from EventBridge rule or direct test invoke):
        {"date": "YYYY-MM-DD"}   -- optional; defaults to yesterday UTC

    Returns a dict with per-tenant summaries. T5.4 will publish each summary
    to SNS; T5.5 will format and send the SES email.
    """
    report_date = event.get("date") or _yesterday_utc()

    logger.info(json.dumps({
        "message": "summary lambda invoked",
        "report_date": report_date,
        "request_id": context.aws_request_id,
    }))

    tenants = _get_active_tenants()
    logger.info(json.dumps({
        "message": f"processing {len(tenants)} active tenants",
        "report_date": report_date,
    }))

    summaries = []
    for tenant in tenants:
        tenant_id = tenant["tenantId"]
        try:
            summary = _aggregate_tenant(tenant_id, report_date, tenant)
            summaries.append(summary)
            logger.info(json.dumps({
                "message": "tenant summary complete",
                "tenantId": tenant_id,
                "totalEvents": summary["totalEvents"],
            }))
        except Exception as exc:
            logger.error(json.dumps({
                "message": "failed to aggregate tenant; skipping",
                "tenantId": tenant_id,
                "error": str(exc),
            }))

    return {
        "reportDate": report_date,
        "tenantsProcessed": len(summaries),
        "summaries": summaries,
    }


# ── Helpers ───────────────────────────────────────────────────────────────────

def _yesterday_utc() -> str:
    """Returns yesterday's date in UTC as YYYY-MM-DD."""
    return (date.today() - timedelta(days=1)).isoformat()


def _get_active_tenants() -> list:
    """
    Scans TenantConfigTable for all tenants with status='active'.

    A Scan is intentional and correct here: TenantConfigTable is a small
    administrative table (bounded by the number of tenants we onboard,
    typically tens — not millions). The strict no-Scan rule applies to the
    Events table, where a Scan would iterate over millions of unfiltered rows.
    """
    table = dynamodb.Table(TENANT_TABLE)
    results = []
    kwargs = {
        "FilterExpression": "attribute_exists(tenantId)",
        "ProjectionExpression":
            "tenantId, displayName, reportRecipients, activeEventTypes, #st",
        "ExpressionAttributeNames": {"#st": "status"},  # 'status' is a reserved word
    }

    while True:
        resp = table.scan(**kwargs)
        results.extend(resp.get("Items", []))
        last = resp.get("LastEvaluatedKey")
        if not last:
            break
        kwargs["ExclusiveStartKey"] = last

    return [t for t in results if t.get("status") == "active"]


def _aggregate_tenant(tenant_id: str, report_date: str, config: dict) -> dict:
    """
    Queries GSI-1 (tenantId PK, ingestedAt SK) for one full calendar day
    and computes the FR-REP-02 fields.

    Key design note: we query by ingestedAt range (the stable, server-side
    timestamp stamped by IngestFunction) rather than processedAt (which can
    shift on retries). This guarantees that every event submitted on a given
    day appears in that day's report exactly once.

    ConsistentRead is False: GSIs are always eventually consistent in DynamoDB
    and do not support strong consistency. For daily aggregation (run hours
    after the events were written) eventual consistency is perfectly fine.
    """
    start = f"{report_date}T00:00:00Z"
    end   = f"{report_date}T23:59:59Z"

    table  = dynamodb.Table(EVENTS_TABLE)
    items  = []
    kwargs = {
        "IndexName": EVENTS_GSI,
        "KeyConditionExpression":
            Key("GSI1PK").eq(tenant_id) & Key("GSI1SK").between(start, end),
        "ConsistentRead": False,
    }

    while True:
        resp = table.query(**kwargs)
        items.extend(resp.get("Items", []))
        last = resp.get("LastEvaluatedKey")
        if not last:
            break
        kwargs["ExclusiveStartKey"] = last

    # ── Aggregation ───────────────────────────────────────────────────────────

    total = len(items)

    # All items in DynamoDB have status='processed' — rejected events are
    # routed to the ValidationDLQ by Step Functions and never written here.
    # Per-day rejection counts require CloudWatch metrics (Phase 6).
    type_counts = Counter(item.get("eventType", "unknown") for item in items)
    processed   = sum(1 for item in items if item.get("status") == "processed")
    top5        = [
        {"eventType": et, "count": c}
        for et, c in type_counts.most_common(5)
    ]

    latency_p50, latency_p99 = _percentiles(_compute_latencies(items))
    zero_rate_periods         = _find_zero_rate_periods(items)

    # reportRecipients are operational addresses (ops team), not end-user PII.
    # They are included here so T5.5 (SES delivery) can route without a second
    # table lookup. They do NOT appear in the tenant-facing email body.
    return {
        "tenantId":          tenant_id,
        "displayName":       config.get("displayName", tenant_id),
        "reportDate":        report_date,
        "totalEvents":       total,
        "byEventType":       dict(type_counts),
        "processed":         processed,
        "rejected":          "deferred to Phase 6",  # requires CW metrics
        "top5EventTypes":    top5,
        "latencyMs": {
            "p50": latency_p50,
            "p99": latency_p99,
        },
        "zeroRatePeriods":   zero_rate_periods,
        "reportRecipients":  list(config.get("reportRecipients", [])),
    }


def _compute_latencies(items: list) -> list:
    """
    Computes ingestedAt → processedAt latency in milliseconds for each item.
    Skips items where either timestamp is absent or unparseable.
    No PII involved — both fields are pipeline-side timestamps, not user data.
    """
    latencies = []
    for item in items:
        ingested_at  = item.get("ingestedAt",  "")
        processed_at = item.get("processedAt", "")
        if not ingested_at or not processed_at:
            continue
        try:
            t_in  = datetime.fromisoformat(ingested_at.replace("Z",  "+00:00"))
            t_out = datetime.fromisoformat(processed_at.replace("Z", "+00:00"))
            ms = int((t_out - t_in).total_seconds() * 1000)
            if ms >= 0:
                latencies.append(ms)
        except (ValueError, AttributeError):
            continue
    return latencies


def _percentiles(values: list) -> tuple:
    """
    Returns (p50, p99) of the input list. Returns (None, None) if empty.
    Uses nearest-rank method: p50 = value at index floor(0.50 * n), etc.
    """
    if not values:
        return None, None
    s = sorted(values)
    n = len(s)
    return s[int(0.50 * n)], s[min(int(0.99 * n), n - 1)]


def _find_zero_rate_periods(items: list) -> list:
    """
    Identifies gaps > 5 consecutive minutes between successive ingestedAt
    timestamps. An empty list means no outages detected for the period.

    A gap is reported as {"from": ISO8601, "to": ISO8601, "durationMinutes": int}.
    """
    GAP_THRESHOLD_S = 5 * 60

    timestamps = []
    for item in items:
        ts = item.get("ingestedAt", "")
        if ts:
            try:
                timestamps.append(
                    datetime.fromisoformat(ts.replace("Z", "+00:00"))
                )
            except (ValueError, AttributeError):
                continue

    if len(timestamps) < 2:
        return []

    timestamps.sort()
    periods = []
    for i in range(1, len(timestamps)):
        gap = (timestamps[i] - timestamps[i - 1]).total_seconds()
        if gap > GAP_THRESHOLD_S:
            periods.append({
                "from":            timestamps[i - 1].strftime("%Y-%m-%dT%H:%M:%SZ"),
                "to":              timestamps[i].strftime("%Y-%m-%dT%H:%M:%SZ"),
                "durationMinutes": int(gap // 60),
            })
    return periods
