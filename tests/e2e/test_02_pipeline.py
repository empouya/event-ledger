# tests/e2e/test_02_pipeline.py
#
# Area: Core pipeline -- ingest -> Kinesis -> Consumer -> State Machine
#       -> DynamoDB + S3  (sections 2-10)
#
# NOTE: LocalStack Community REQUEST authorizer does not inject context into
# downstream Lambdas. Sections 2, 8, 9, 10 bypass API GW and invoke
# IngestFunction/ConsumerFunction/WriterFunction directly.
import json
import re
import time

from _helpers import (
    SoftChecks,
    find_kinesis_record_by_ingestion_id,
    kinesis_event_envelope,
    load_fixture,
    now_iso,
    wait_for_execution,
)


def test_pipeline(aws_clients, stack, valid_token, invoke_lambda):
    checks = SoftChecks()
    dynamodb = aws_clients["dynamodb"]
    kinesis = aws_clients["kinesis"]
    sfn = aws_clients["stepfunctions"]
    sqs = aws_clients["sqs"]
    s3 = aws_clients["s3"]

    # =========================================================================
    # 2. POST VALID EVENT (via direct Lambda invoke -- see bypass note above)
    # =========================================================================
    event_obj = load_fixture("valid", "order.placed.json")
    event_obj["clientTimestamp"] = now_iso()

    s2_payload = {
        "body": json.dumps(event_obj),
        "headers": {"authorization": f"Bearer {valid_token}"},
        "requestContext": {"authorizer": {"tenantId": "tenant_test", "tenantRole": "sdk_writer"}},
    }
    _, s2_resp = invoke_lambda(stack["ingest_fn"], s2_payload)
    ingest = json.loads(s2_resp["body"])

    checks.check(s2_resp["statusCode"] == 202, "response status = accepted (202)",
                 f"expected 202, got {s2_resp['statusCode']}")

    ingestion_id = ingest["ingestionId"]
    ingested_at = ingest["timestamp"]
    event_id = event_obj["eventId"]

    # =========================================================================
    # 3. READ REAL KINESIS RECORD
    # =========================================================================
    matching_record = find_kinesis_record_by_ingestion_id(kinesis, stack["stream_name"], ingestion_id)
    checks.check(matching_record is not None,
                 f"real Kinesis record found for ingestionId {ingestion_id}",
                 f"no record with ingestionId {ingestion_id} found in stream")

    # =========================================================================
    # 4. INVOKE CONSUMER LAMBDA WITH THE REAL RECORD
    # =========================================================================
    kinesis_event = kinesis_event_envelope(matching_record, stack["stream_name"])
    resp, _ = invoke_lambda(stack["consumer_fn"], kinesis_event)
    checks.check(resp["StatusCode"] == 200 and "FunctionError" not in resp,
                 "Consumer Lambda invoked successfully",
                 f"StatusCode={resp['StatusCode']} FunctionError={resp.get('FunctionError')}")

    exec_ = wait_for_execution(sfn, stack["sm_arn"], ingestion_id)

    # =========================================================================
    # 5. STATE MACHINE: ASSERT SUCCEEDED
    # =========================================================================
    if exec_:
        checks.check(exec_["status"] == "SUCCEEDED", "execution SUCCEEDED",
                     f"expected SUCCEEDED, got '{exec_['status']}'")
    else:
        checks.check(False, "execution lookup", f"no execution found with name {ingestion_id}")

    # =========================================================================
    # 6. DYNAMODB ASSERTIONS
    # =========================================================================
    items = dynamodb.query(
        TableName="streamcore-events-dev",
        KeyConditionExpression="PK = :pk",
        ExpressionAttributeValues={":pk": {"S": "tenant_test#order.placed"}},
    )["Items"]
    item = next((i for i in items if i["ingestionId"]["S"] == ingestion_id), None)

    checks.check(item is not None, f"item found for ingestionId {ingestion_id}", "item not in DynamoDB")

    if item is not None:
        checks.check(item["SK"]["S"].startswith(ingested_at), "SK = ingestedAt#eventId (idempotency key correct)",
                     f"expected SK to start with '{ingested_at}', got '{item['SK']['S']}'")
        checks.check(item["status"]["S"] == "processed", "status = processed", item["status"]["S"])
        checks.check(item["pipelineVersion"]["S"] == "2.0.0", "pipelineVersion = 2.0.0", item["pipelineVersion"]["S"])
        checks.check(item["schemaVersionUsed"]["S"] == "1.0", "schemaVersionUsed = 1.0", item["schemaVersionUsed"]["S"])
        checks.check(int(item["expiresAt"]["N"]) > 0, "expiresAt set (TTL)", "expected a non-zero integer")

        payload_parsed = json.loads(item["payload"]["S"])
        hashed_user_id = payload_parsed["userId"]
        checks.check(len(hashed_user_id) == 64 and re.match(r"^[0-9a-f]+$", hashed_user_id),
                     "userId is 64-char hex digest (PII hashed)", f"got '{hashed_user_id}'")
        hashed_email = payload_parsed["userEmail"]
        checks.check(len(hashed_email) == 64 and re.match(r"^[0-9a-f]+$", hashed_email),
                     "userEmail is 64-char hex digest (PII hashed)", f"got '{hashed_email}'")

    # =========================================================================
    # 7. S3 ASSERTION -- key format y/m/d/h + x-streamcore-* metadata
    # =========================================================================
    s3_objects = s3.list_objects(Bucket="streamcore-events-raw-dev").get("Contents", [])
    s3_match = next((o for o in s3_objects if event_id in o["Key"]), None)

    if s3_match:
        checks.check(True, f"S3 object exists: {s3_match['Key']}")

        checks.check(bool(re.match(r"^tenant_test/order\.placed/\d{4}/\d{2}/\d{2}/\d{2}/", s3_match["Key"])),
                     "S3 key has y/m/d/h hierarchy",
                     f"expected tenant/type/yyyy/mm/dd/hh/... hierarchy, got '{s3_match['Key']}'")

        meta = s3.head_object(Bucket="streamcore-events-raw-dev", Key=s3_match["Key"]).get("Metadata", {})

        checks.check(meta.get("x-streamcore-tenant-id") == "tenant_test",
                     "S3 metadata: x-streamcore-tenant-id = tenant_test",
                     f"missing or wrong: '{meta.get('x-streamcore-tenant-id')}'")
        checks.check(meta.get("x-streamcore-event-type") == "order.placed",
                     "S3 metadata: x-streamcore-event-type = order.placed",
                     f"missing or wrong: '{meta.get('x-streamcore-event-type')}'")
        checks.check(meta.get("x-streamcore-schema-version") == "1.0",
                     "S3 metadata: x-streamcore-schema-version = 1.0",
                     f"missing or wrong: '{meta.get('x-streamcore-schema-version')}'")
    else:
        checks.check(False, "S3 object", f"no object found containing eventId '{event_id}'")

    # =========================================================================
    # 8. REJECTED EVENT: events/invalid/bad-currency.json -> INVALID_CURRENCY
    #    -> SM SUCCEEDED -> SendToValidationDLQ -> MessageId in output
    # =========================================================================
    sqs.purge_queue(QueueUrl=stack["validation_dlq_url"])
    time.sleep(1)

    bad_obj = load_fixture("invalid", "bad-currency.json")
    bad_obj.pop("_note", None)
    bad_obj["clientTimestamp"] = now_iso()

    s8_payload = {
        "body": json.dumps(bad_obj),
        "headers": {"authorization": f"Bearer {valid_token}"},
        "requestContext": {"authorizer": {"tenantId": "tenant_test", "tenantRole": "sdk_writer"}},
    }
    _, s8_resp = invoke_lambda(stack["ingest_fn"], s8_payload)
    bad_ingestion_id = json.loads(s8_resp["body"])["ingestionId"]

    time.sleep(1)
    bad_record = find_kinesis_record_by_ingestion_id(kinesis, stack["stream_name"], bad_ingestion_id)

    if bad_record is None:
        checks.check(False, "bad event Kinesis record", f"could not find record for ingestionId {bad_ingestion_id}")
    else:
        bad_envelope = kinesis_event_envelope(bad_record, stack["stream_name"])
        invoke_lambda(stack["consumer_fn"], bad_envelope)

        bad_exec = wait_for_execution(sfn, stack["sm_arn"], bad_ingestion_id, timeout=15, poll_interval=10)

        if bad_exec and bad_exec["status"] == "SUCCEEDED":
            checks.check(True, "rejected event: SM execution SUCCEEDED")

            bad_output = json.loads(sfn.describe_execution(executionArn=bad_exec["executionArn"])["output"])
            checks.check(bool(bad_output.get("MessageId")),
                         f"rejected event routed to SendToValidationDLQ (MessageId: {bad_output.get('MessageId')})",
                         f"expected SQS MessageId in execution output, got: {json.dumps(bad_output)}")
        else:
            checks.check(False, "bad execution",
                         f"expected SUCCEEDED, got '{bad_exec['status'] if bad_exec else 'NOT FOUND'}'")

        dlq_resp = sqs.receive_message(QueueUrl=stack["validation_dlq_url"], MaxNumberOfMessages=1, WaitTimeSeconds=0)
        dlq_msg = dlq_resp.get("Messages", [None])[0]

        if dlq_msg:
            dlq_body = json.loads(dlq_msg["Body"])
            checks.check(dlq_body.get("ingestionId") == bad_ingestion_id, "ValidationDLQ contains rejected event with correct ingestionId",
                         f"expected {bad_ingestion_id}, got '{dlq_body.get('ingestionId')}'")
        else:
            checks.check(False, "ValidationDLQ", "no message received from queue")

        all_items = dynamodb.query(
            TableName="streamcore-events-dev",
            KeyConditionExpression="PK = :pk",
            ExpressionAttributeValues={":pk": {"S": "tenant_test#order.placed"}},
        )["Items"]
        rejected_item = next((i for i in all_items if i["ingestionId"]["S"] == bad_ingestion_id), None)
        checks.check(rejected_item is None, "rejected event absent from DynamoDB",
                     "item found in DynamoDB -- rejected events must not be persisted")

    # =========================================================================
    # 9. IDEMPOTENCY: duplicate Kinesis delivery -> ExecutionAlreadyExists (STANDARD)
    # =========================================================================
    dup_resp, _ = invoke_lambda(stack["consumer_fn"], kinesis_event)
    checks.check(dup_resp["StatusCode"] == 200 and "FunctionError" not in dup_resp,
                 "Consumer Lambda re-invocation did not crash", f"Lambda error: {dup_resp.get('FunctionError')}")

    time.sleep(3)

    final_items = dynamodb.query(
        TableName="streamcore-events-dev",
        KeyConditionExpression="PK = :pk",
        ExpressionAttributeValues={":pk": {"S": "tenant_test#order.placed"}},
    )["Items"]
    dedup_items = [i for i in final_items if i["ingestionId"]["S"] == ingestion_id]
    checks.check(len(dedup_items) == 1, "exactly 1 DynamoDB item after duplicate delivery (idempotent)",
                 f"expected 1 item, found {len(dedup_items)}")

    # =========================================================================
    # 10. WRITER IDEMPOTENCY (direct double-invoke -- attribute_not_exists PK path)
    #
    #  Section 9 tests Consumer-level dedup (ExecutionAlreadyExists), STANDARD only.
    #  On real AWS with EXPRESS executions, duplicates CAN reach the Writer.
    #  Invoke WriterFunction twice with the same event (same PK+SK).
    #  Second call hits ConditionalCheckFailedException; Writer suppresses it.
    #  Assert: no Lambda error on either call; exactly 1 DynamoDB item.
    # =========================================================================
    idemp_ts = "2020-01-01T00:00:00.000Z"
    idemp_event = {
        "eventId": "bb0e8400-e29b-41d4-a716-446655440099",
        "eventType": "user.login",
        "schemaVersion": "1.0",
        "schemaVersionUsed": "1.0",
        "clientTimestamp": idemp_ts,
        "ingestedAt": idemp_ts,
        "processedAt": idemp_ts,
        "tenantId": "tenant_test",
        "ingestionId": "idemp-direct-001",
        "pipelineVersion": "2.0.0",
        "payload": {"source": "web"},
    }

    r1, _ = invoke_lambda(stack["writer_fn"], idemp_event)
    checks.check("FunctionError" not in r1, "Writer first invocation: no error", f"FunctionError={r1.get('FunctionError')}")

    r2, _ = invoke_lambda(stack["writer_fn"], idemp_event)
    checks.check("FunctionError" not in r2, "Writer second invocation (duplicate): ConditionalCheckFailed suppressed, no error",
                 f"FunctionError={r2.get('FunctionError')}")

    idemp_item = dynamodb.get_item(
        TableName="streamcore-events-dev",
        Key={"PK": {"S": "tenant_test#user.login"}, "SK": {"S": f"{idemp_ts}#bb0e8400-e29b-41d4-a716-446655440099"}},
    ).get("Item")

    checks.check(
        idemp_item is not None and idemp_item["eventId"]["S"] == "bb0e8400-e29b-41d4-a716-446655440099",
        "Writer idempotency: item exists at exact PK+SK after double-invoke (attribute_not_exists guard)",
        f"item not found at PK=tenant_test#user.login SK={idemp_ts}#bb0e8400...",
    )

    checks.done()
