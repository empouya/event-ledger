#!/usr/bin/env python3
"""Push the event fixtures through the pipeline.

Reads every JSON file in events/valid/ (and optionally events/invalid/),
stamps it with a fresh tenantId / timestamps / ingestionId, and starts one
Step Functions execution per event. This is the manual "give me some data"
helper -- the e2e suite seeds its own events.

Usage:
    python3 scripts/seed_events.py                      # valid events, tenant_dev (all types allowed)
    python3 scripts/seed_events.py --tenant tenant_test  # only order.placed / user.login pass VAL-003
    python3 scripts/seed_events.py --include-invalid     # also send the invalid fixtures (go to ValidationDLQ)

Prerequisite: ./dev-deploy.sh && source ./dev-post-deploy.sh  (deploys the stack and seeds configs)
"""
import argparse
import json
import sys
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path

import boto3

REGION = "eu-west-1"
LOCALSTACK_ENDPOINT = "http://localhost:4566"
STACK_NAME = "streamcore-local"
EVENTS_ROOT = Path(__file__).resolve().parent.parent / "events"


def _client(service):
    return boto3.client(
        service, endpoint_url=LOCALSTACK_ENDPOINT, region_name=REGION,
        aws_access_key_id="test", aws_secret_access_key="test",
    )


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--tenant", default="tenant_dev")
    parser.add_argument("--include-invalid", action="store_true")
    args = parser.parse_args()

    cfn = _client("cloudformation")
    sfn = _client("stepfunctions")
    dynamodb = _client("dynamodb")

    outputs = {
        o["OutputKey"]: o["OutputValue"]
        for o in cfn.describe_stacks(StackName=STACK_NAME)["Stacks"][0].get("Outputs", [])
    }
    sm_arn = outputs.get("ProcessingStateMachineArn")
    if not sm_arn:
        print("ProcessingStateMachineArn not found -- is the stack deployed?", file=sys.stderr)
        sys.exit(1)

    dirs = [EVENTS_ROOT / "valid"]
    if args.include_invalid:
        dirs.append(EVENTS_ROOT / "invalid")

    now = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"
    arns = []

    for directory in dirs:
        for file in sorted(directory.glob("*.json")):
            obj = json.loads(file.read_text())

            # Stamp the pipeline-side fields the real Ingest Lambda would add.
            # clientTimestamp -> now so the Validator's freshness window passes.
            obj["tenantId"] = args.tenant
            obj["clientTimestamp"] = now
            obj["ingestedAt"] = now
            obj["ingestionId"] = str(uuid.uuid4())

            exec_ = sfn.start_execution(stateMachineArn=sm_arn, input=json.dumps(obj))
            print(f"started  {file.name:<22} ({directory.name})")
            arns.append(exec_["executionArn"])

    print("\nWaiting for executions to settle...")
    time.sleep(8)

    ok = other = 0
    for arn in arns:
        status = sfn.describe_execution(executionArn=arn)["status"]
        if status == "SUCCEEDED":
            ok += 1
        else:
            other += 1
    print(f"Executions: {ok} SUCCEEDED, {other} other")
    print("(Invalid fixtures also report SUCCEEDED -- they are caught and routed to the ValidationDLQ, not failed.)")

    print("\nEvents now in DynamoDB:")
    count = dynamodb.scan(TableName="streamcore-events-dev", Select="COUNT")["Count"]
    print(count)


if __name__ == "__main__":
    main()
