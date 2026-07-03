# tests/e2e/_helpers.py
#
# Plain (non-fixture) helpers shared across the e2e test modules: fixture
# loading, timestamp formatting, and the Kinesis/Step Functions polling
# patterns every area file used against the real stack.
import base64
import json
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

FIXTURES_ROOT = Path(__file__).resolve().parent.parent.parent / "events"


def load_fixture(*parts) -> dict:
    with open(FIXTURES_ROOT.joinpath(*parts)) as f:
        return json.load(f)


def now_iso() -> str:
    """yyyy-MM-ddTHH:mm:ss.fffZ, matching the .NET format the PowerShell suite used."""
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"


def yesterday_date() -> str:
    return (datetime.now(timezone.utc) - timedelta(days=1)).strftime("%Y-%m-%d")


def find_kinesis_record_by_ingestion_id(kinesis_client, stream_name, ingestion_id, timeout=10):
    """Poll TRIM_HORIZON records on the single shard for one whose body matches ingestion_id.
    boto3 already base64-decodes the blob 'Data' field to raw bytes (unlike the AWS CLI's
    JSON output, which leaves it base64-encoded and required a manual decode step)."""
    deadline = time.time() + timeout
    while True:
        shard_iter = kinesis_client.get_shard_iterator(
            StreamName=stream_name,
            ShardId="shardId-000000000000",
            ShardIteratorType="TRIM_HORIZON",
        )["ShardIterator"]
        records = kinesis_client.get_records(ShardIterator=shard_iter)["Records"]
        for r in records:
            data = json.loads(r["Data"])
            if data.get("ingestionId") == ingestion_id:
                return r
        if time.time() >= deadline:
            return None
        time.sleep(1)


def kinesis_event_envelope(record, stream_name):
    """Wrap a raw get-records Record dict as a Lambda Kinesis-trigger event, for direct
    Consumer invocation (LocalStack Community's ESM does not restart polling after the
    first post-deploy batch -- see ADR / ledger 'ongoing' risk)."""
    return {
        "Records": [{
            "kinesis": {
                "data": record["Data"] if isinstance(record["Data"], str) else base64.b64encode(record["Data"]).decode(),
                "partitionKey": record["PartitionKey"],
                "sequenceNumber": record["SequenceNumber"],
                "approximateArrivalTimestamp": 1718356800,
                "kinesisSchemaVersion": "1.0",
            },
            "eventSource": "aws:kinesis",
            "eventSourceARN": f"arn:aws:kinesis:eu-west-1:000000000000:stream/{stream_name}",
            "awsRegion": "eu-west-1",
            "eventID": f"shardId-000000000000:{record['SequenceNumber']}",
            "eventName": "aws:kinesis:record",
            "invokeIdentityArn": "arn:aws:iam::000000000000:role/lambda-role",
            "eventVersion": "1.0",
        }]
    }


class SoftChecks:
    """Accumulates pass/fail across a whole multi-step scenario instead of stopping at the
    first failed assertion -- each area file is one coherent end-to-end flow (later steps
    depend on earlier ones), and the old PowerShell suite's useful property was reporting
    every failure in the flow, not just the first. Call .done() at the end of the test."""

    def __init__(self):
        self.failures = []

    def check(self, condition, label, detail=""):
        if condition:
            print(f"[PASS] {label}")
        else:
            msg = f"[FAIL] {label}" + (f" -- {detail}" if detail else "")
            print(msg)
            self.failures.append(msg)

    def done(self):
        assert not self.failures, "\n" + "\n".join(self.failures)


def wait_for_execution(sfn_client, state_machine_arn, execution_name, timeout=90, poll_interval=5):
    """Poll list-executions by name until it's no longer RUNNING or timeout elapses.
    Always waits at least one poll_interval, mirroring the original do/while."""
    deadline = time.time() + timeout
    execution = None
    while True:
        time.sleep(poll_interval)
        executions = sfn_client.list_executions(stateMachineArn=state_machine_arn)["executions"]
        execution = next((e for e in executions if e["name"] == execution_name), None)
        if not execution or execution["status"] != "RUNNING" or time.time() >= deadline:
            break
    return execution
