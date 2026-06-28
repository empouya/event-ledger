import json
import logging
import os
from collections import Counter
from datetime import datetime, date, timedelta, timezone

import boto3
from boto3.dynamodb.conditions import Key

from aws_xray_sdk.core import xray_recorder, patch_all
patch_all()

logger = logging.getLogger()
logger.setLevel(logging.INFO)

# ── Module-scope resources (created once per execution environment) ────────────
dynamodb         = boto3.resource("dynamodb")
EVENTS_TABLE     = os.environ["EVENTS_TABLE"]
EVENTS_GSI       = os.environ.get("EVENTS_GSI", "GSI1")
TENANT_TABLE     = os.environ["TENANT_CONFIG_TABLE"]
REPORTS_TOPIC_ARN = os.environ.get("REPORTS_TOPIC_ARN", "")
sns = boto3.client("sns")
ses           = boto3.client("ses")
SENDER_EMAIL  = os.environ.get("SENDER_EMAIL", "")
OPS_EMAIL     = os.environ.get("OPS_EMAIL", "")


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

    for summary in summaries:
        _publish_tenant_report(summary)
        _send_tenant_email(summary)

    if summaries:
        _send_ops_report(summaries, report_date)

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


def _publish_tenant_report(summary: dict) -> None:
    """
    Publishes one tenant's summary to the daily-reports SNS topic.
    The tenantId message attribute lets subscriptions use filter policies
    so each subscriber only receives their own tenant's messages.
    Skips silently if REPORTS_TOPIC_ARN is not set (unit-test / local-only mode).
    """
    if not REPORTS_TOPIC_ARN:
        logger.warning(json.dumps({
            "message": "REPORTS_TOPIC_ARN not set; skipping SNS publish",
            "tenantId": summary.get("tenantId"),
        }))
        return
    try:
        sns.publish(
            TopicArn=REPORTS_TOPIC_ARN,
            Message=json.dumps(summary),
            Subject=f"StreamCore Daily Report - {summary.get('displayName', summary.get('tenantId'))} - {summary.get('reportDate')}",
            MessageAttributes={
                "tenantId": {
                    "DataType": "String",
                    "StringValue": summary.get("tenantId", ""),
                },
            },
        )
        logger.info(json.dumps({
            "message": "tenant report published to SNS",
            "tenantId": summary.get("tenantId"),
            "reportDate": summary.get("reportDate"),
        }))
    except Exception as exc:
        logger.error(json.dumps({
            "message": "SNS publish failed",
            "tenantId": summary.get("tenantId"),
            "error": str(exc),
        }))


def _send_tenant_email(summary: dict) -> None:
    """
    Sends the per-tenant daily report to reportRecipients.
    Skips with WARN if no recipients are configured.
    Body contains only aggregate statistics — no PII (no userId, no raw emails in body).
    Subject: StreamCore Daily Report - {displayName} - {YYYY-MM-DD}
    """
    recipients = list(summary.get("reportRecipients", []))
    if not recipients:
        logger.warning(json.dumps({
            "message": "no reportRecipients configured; skipping email",
            "tenantId": summary.get("tenantId"),
        }))
        return
    if not SENDER_EMAIL:
        logger.warning(json.dumps({"message": "SENDER_EMAIL not set; skipping tenant email"}))
        return

    tenant_id    = summary.get("tenantId", "")
    display_name = summary.get("displayName", tenant_id)
    report_date  = summary.get("reportDate", "")
    subject      = f"StreamCore Daily Report - {display_name} - {report_date}"
    latency      = summary.get("latencyMs", {})

    top5_text = "\n".join(
        f"  {i+1}. {e['eventType']}: {e['count']}"
        for i, e in enumerate(summary.get("top5EventTypes", []))
    ) or "  (none)"

    top5_html = "".join(
        f"<li>{e['eventType']}: {e['count']}</li>"
        for e in summary.get("top5EventTypes", [])
    )

    text_body = (
        f"StreamCore Daily Report\n"
        f"Tenant: {display_name}\n"
        f"Date:   {report_date}\n\n"
        f"Total events:  {summary.get('totalEvents', 0)}\n"
        f"Processed:     {summary.get('processed', 0)}\n"
        f"Rejected:      {summary.get('rejected', 'deferred')}\n\n"
        f"Top event types:\n{top5_text}\n\n"
        f"Pipeline latency:\n"
        f"  p50: {latency.get('p50')} ms\n"
        f"  p99: {latency.get('p99')} ms\n"
    )
    html_body = (
        f"<h2>StreamCore Daily Report</h2>"
        f"<p><strong>Tenant:</strong> {display_name} &nbsp;|&nbsp; <strong>Date:</strong> {report_date}</p>"
        f"<table border='1' cellpadding='4' style='border-collapse:collapse'>"
        f"<tr><th>Metric</th><th>Value</th></tr>"
        f"<tr><td>Total events</td><td>{summary.get('totalEvents', 0)}</td></tr>"
        f"<tr><td>Processed</td><td>{summary.get('processed', 0)}</td></tr>"
        f"<tr><td>Rejected</td><td>{summary.get('rejected', 'deferred')}</td></tr>"
        f"<tr><td>p50 latency</td><td>{latency.get('p50')} ms</td></tr>"
        f"<tr><td>p99 latency</td><td>{latency.get('p99')} ms</td></tr>"
        f"</table>"
        f"<h3>Top event types</h3><ol>{top5_html}</ol>"
    )

    try:
        ses.send_email(
            Source=SENDER_EMAIL,
            Destination={"ToAddresses": recipients},
            Message={
                "Subject": {"Data": subject, "Charset": "UTF-8"},
                "Body": {
                    "Text": {"Data": text_body, "Charset": "UTF-8"},
                    "Html": {"Data": html_body, "Charset": "UTF-8"},
                },
            },
        )
        logger.info(json.dumps({
            "message": "tenant report email sent",
            "tenantId": tenant_id,
            "recipientCount": len(recipients),
        }))
    except Exception as exc:
        logger.error(json.dumps({
            "message": "SES send_email failed",
            "tenantId": tenant_id,
            "error": str(exc),
        }))


def _send_ops_report(summaries: list, report_date: str) -> None:
    """
    Sends a single internal ops report covering all tenants.
    Cost Explorer cost line is deferred to Phase 6.
    """
    if not SENDER_EMAIL or not OPS_EMAIL:
        logger.warning(json.dumps({"message": "SENDER_EMAIL or OPS_EMAIL not set; skipping ops report"}))
        return

    total_events    = sum(s.get("totalEvents", 0) for s in summaries)
    total_processed = sum(s.get("processed", 0) for s in summaries)
    subject         = f"StreamCore Internal Ops Report - {report_date}"

    rows_text = "\n".join(
        f"  {s.get('displayName', s.get('tenantId'))}: "
        f"{s.get('totalEvents', 0)} events, {s.get('processed', 0)} processed"
        for s in summaries
    )
    rows_html = "".join(
        f"<tr><td>{s.get('displayName', s.get('tenantId'))}</td>"
        f"<td>{s.get('totalEvents', 0)}</td>"
        f"<td>{s.get('processed', 0)}</td></tr>"
        for s in summaries
    )

    text_body = (
        f"StreamCore Internal Ops Report\n"
        f"Date: {report_date}\n\n"
        f"All-tenant totals:\n"
        f"  Events:    {total_events}\n"
        f"  Processed: {total_processed}\n\n"
        f"Per-tenant breakdown:\n{rows_text}\n\n"
        f"Note: Cost Explorer cost line deferred to Phase 6.\n"
    )
    html_body = (
        f"<h2>StreamCore Internal Ops Report</h2>"
        f"<p><strong>Date:</strong> {report_date}</p>"
        f"<p>All-tenant totals &mdash; "
        f"Events: <strong>{total_events}</strong> | "
        f"Processed: <strong>{total_processed}</strong></p>"
        f"<table border='1' cellpadding='4' style='border-collapse:collapse'>"
        f"<tr><th>Tenant</th><th>Events</th><th>Processed</th></tr>"
        f"{rows_html}"
        f"</table>"
        f"<p><em>Cost Explorer cost line deferred to Phase 6.</em></p>"
    )

    try:
        ses.send_email(
            Source=SENDER_EMAIL,
            Destination={"ToAddresses": [OPS_EMAIL]},
            Message={
                "Subject": {"Data": subject, "Charset": "UTF-8"},
                "Body": {
                    "Text": {"Data": text_body, "Charset": "UTF-8"},
                    "Html": {"Data": html_body, "Charset": "UTF-8"},
                },
            },
        )
        logger.info(json.dumps({
            "message": "ops report email sent",
            "reportDate": report_date,
            "totalEvents": total_events,
        }))
    except Exception as exc:
        logger.error(json.dumps({
            "message": "SES ops report send_email failed",
            "error": str(exc),
        }))
