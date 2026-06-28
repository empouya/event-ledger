# tests/e2e/02_pipeline.ps1
#
# Area: Core pipeline -- ingest -> Kinesis -> Consumer -> State Machine
#       -> DynamoDB + S3  (sections 2-10)
#
# NOTE: LocalStack Community REQUEST authorizer does not inject context into
# downstream Lambdas.  Sections 2, 8, 9, 10 bypass API GW and invoke
# IngestFunction/ConsumerFunction/WriterFunction directly.

. "$PSScriptRoot\_common.ps1"

# =============================================================================
# 2. POST VALID EVENT (via direct Lambda invoke -- see bypass note above)
# =============================================================================
Write-Host "`n=== 2. POST /v1/events (valid) ==="
$eventObj = Get-Content (Join-Path $FixturesRoot "valid\order.placed.json") -Raw | ConvertFrom-Json
$eventObj | Add-Member -NotePropertyName clientTimestamp `
    -NotePropertyValue ((Get-Date).ToUniversalTime().ToString("yyyy-MM-ddTHH:mm:ss.fffZ")) -Force

$s2Payload = @{
    body           = ($eventObj | ConvertTo-Json -Depth 10 -Compress)
    headers        = @{ authorization = "Bearer $validToken" }
    requestContext = @{ authorizer = @{ tenantId = "tenant_test"; tenantRole = "sdk_writer" } }
} | ConvertTo-Json -Compress -Depth 10

$s2Resp  = Invoke-Lambda $ingestFn $s2Payload
$ingest  = $s2Resp.body | ConvertFrom-Json

if ($s2Resp.statusCode -eq 202) { Pass "response status = accepted (202)" }
else { Fail "response status" "expected 202, got $($s2Resp.statusCode)" }

$ingestionId = $ingest.ingestionId
$ingestedAt  = $ingest.timestamp
$eventId     = $eventObj.eventId
Write-Host "    ingestionId: $ingestionId"
Write-Host "    ingestedAt:  $ingestedAt"
Write-Host "    eventId:     $eventId"

# =============================================================================
# 3. READ REAL KINESIS RECORD
# =============================================================================
Write-Host "`n=== 3. Read real Kinesis record ==="
$shardIterator = (awslocal kinesis get-shard-iterator `
    --stream-name streamcore-events-dev `
    --shard-id shardId-000000000000 `
    --shard-iterator-type TRIM_HORIZON `
    --output json | ConvertFrom-Json).ShardIterator

$allRecords = (awslocal kinesis get-records `
    --shard-iterator $shardIterator `
    --output json | ConvertFrom-Json).Records

$matchingRecord = $allRecords | Where-Object {
    $data = [System.Text.Encoding]::UTF8.GetString([Convert]::FromBase64String($_.Data))
    ($data | ConvertFrom-Json).ingestionId -eq $ingestionId
} | Select-Object -First 1

if ($matchingRecord) { Pass "real Kinesis record found for ingestionId $ingestionId" }
else                  { Fail "Kinesis record" "no record with ingestionId $ingestionId found in stream" }

# =============================================================================
# 4. INVOKE CONSUMER LAMBDA WITH THE REAL RECORD
# =============================================================================
Write-Host "`n=== 4. Consumer Lambda ==="
$kinesisEvent = @{
    Records = @(@{
        kinesis = @{
            data                        = $matchingRecord.Data
            partitionKey                = $matchingRecord.PartitionKey
            sequenceNumber              = $matchingRecord.SequenceNumber
            approximateArrivalTimestamp = 1718356800
            kinesisSchemaVersion        = "1.0"
        }
        eventSource       = "aws:kinesis"
        eventSourceARN    = "arn:aws:kinesis:eu-west-1:000000000000:stream/streamcore-events-dev"
        awsRegion         = "eu-west-1"
        eventID           = "shardId-000000000000:$($matchingRecord.SequenceNumber)"
        eventName         = "aws:kinesis:record"
        invokeIdentityArn = "arn:aws:iam::000000000000:role/lambda-role"
        eventVersion      = "1.0"
    })
} | ConvertTo-Json -Depth 10 -Compress

$eventFile    = Join-Path $env:TEMP "sc_kinesis_event.json"
$responseFile = Join-Path $env:TEMP "sc_consumer_response.json"
$kinesisEvent | Set-Content $eventFile -Encoding ASCII

$invokeResult = awslocal lambda invoke `
    --function-name $consumerFn `
    --payload "file://$eventFile" `
    --output json `
    $responseFile | ConvertFrom-Json
Remove-Item $eventFile, $responseFile -ErrorAction SilentlyContinue

$funcError = if ($invokeResult.PSObject.Properties['FunctionError']) { $invokeResult.FunctionError } else { $null }
if ($invokeResult.StatusCode -eq 200 -and -not $funcError) { Pass "Consumer Lambda invoked successfully" }
else { Fail "Consumer Lambda" "StatusCode=$($invokeResult.StatusCode) FunctionError=$funcError" }

Write-Host "    waiting for state machine..."
Start-Sleep -Seconds 20

# =============================================================================
# 5. STATE MACHINE: ASSERT SUCCEEDED
# =============================================================================
Write-Host "`n=== 5. State machine execution ==="
$exec = (awslocal stepfunctions list-executions `
    --state-machine-arn $smArn `
    --output json | ConvertFrom-Json).executions |
    Where-Object { $_.name -eq $ingestionId } | Select-Object -First 1

if ($exec) {
    if ($exec.status -eq "SUCCEEDED") { Pass "execution SUCCEEDED" }
    else                               { Fail "execution status" "expected SUCCEEDED, got '$($exec.status)'" }
} else {
    Fail "execution lookup" "no execution found with name $ingestionId"
}

# =============================================================================
# 6. DYNAMODB ASSERTIONS
# =============================================================================
Write-Host "`n=== 6. DynamoDB assertions ==="
$exprFile = Join-Path $env:TEMP "sc_expr.json"
@{ ':pk' = @{ S = 'tenant_test#order.placed' } } | ConvertTo-Json -Compress | Set-Content $exprFile -Encoding ASCII
$items = (awslocal dynamodb query `
    --table-name streamcore-events-dev `
    --key-condition-expression "PK = :pk" `
    --expression-attribute-values "file://$exprFile" `
    --output json | ConvertFrom-Json).Items
Remove-Item $exprFile

$item = $items | Where-Object { $_.ingestionId.S -eq $ingestionId }

if ($null -ne $item) { Pass "item found for ingestionId $ingestionId" }
else                  { Fail "item lookup" "item not in DynamoDB" }

if ($null -ne $item) {
    if ($item.SK.S.StartsWith($ingestedAt)) { Pass "SK = ingestedAt#eventId (idempotency key correct)" }
    else { Fail "SK format" "expected SK to start with '$ingestedAt', got '$($item.SK.S)'" }

    if ($item.status.S -eq "processed")           { Pass "status = processed" }
    else                                            { Fail "status" $item.status.S }

    if ($item.pipelineVersion.S -eq "2.0.0")      { Pass "pipelineVersion = 2.0.0" }
    else                                            { Fail "pipelineVersion" $item.pipelineVersion.S }

    if ($item.schemaVersionUsed.S -eq "1.0")      { Pass "schemaVersionUsed = 1.0" }
    else                                            { Fail "schemaVersionUsed" $item.schemaVersionUsed.S }

    if ([long]$item.expiresAt.N -gt 0)            { Pass "expiresAt set (TTL)" }
    else                                            { Fail "expiresAt" "expected a non-zero integer" }

    $payloadParsed = $item.payload.S | ConvertFrom-Json
    $hashedUserId  = $payloadParsed.userId
    if ($hashedUserId.Length -eq 64 -and $hashedUserId -match '^[0-9a-f]+$') {
        Pass "userId is 64-char hex digest (PII hashed)"
    } else {
        Fail "PII: userId" "expected 64-char hex, got '$hashedUserId'"
    }
    $hashedEmail = $payloadParsed.userEmail
    if ($hashedEmail.Length -eq 64 -and $hashedEmail -match '^[0-9a-f]+$') {
        Pass "userEmail is 64-char hex digest (PII hashed)"
    } else {
        Fail "PII: userEmail" "expected 64-char hex, got '$hashedEmail'"
    }
}

# =============================================================================
# 7. S3 ASSERTION -- key format y/m/d/h + x-streamcore-* metadata
# =============================================================================
Write-Host "`n=== 7. S3 assertion ==="
$s3Objects = (awslocal s3api list-objects `
    --bucket streamcore-events-raw-dev `
    --output json | ConvertFrom-Json).Contents

$s3Match = $s3Objects | Where-Object { $_.Key -like "*$eventId*" } | Select-Object -First 1

if ($s3Match) {
    Pass "S3 object exists: $($s3Match.Key)"

    if ($s3Match.Key -match "^tenant_test/order\.placed/\d{4}/\d{2}/\d{2}/\d{2}/") {
        Pass "S3 key has y/m/d/h hierarchy"
    } else {
        Fail "S3 key format" "expected tenant/type/yyyy/mm/dd/hh/... hierarchy, got '$($s3Match.Key)'"
    }

    $s3Head = awslocal s3api head-object `
        --bucket streamcore-events-raw-dev `
        --key $s3Match.Key `
        --output json | ConvertFrom-Json
    $meta = $s3Head.Metadata

    if ($meta."x-streamcore-tenant-id" -eq "tenant_test") { Pass "S3 metadata: x-streamcore-tenant-id = tenant_test" }
    else { Fail "S3 metadata" "x-streamcore-tenant-id missing or wrong: '$($meta.'x-streamcore-tenant-id')'" }

    if ($meta."x-streamcore-event-type" -eq "order.placed") { Pass "S3 metadata: x-streamcore-event-type = order.placed" }
    else { Fail "S3 metadata" "x-streamcore-event-type missing or wrong: '$($meta.'x-streamcore-event-type')'" }

    if ($meta."x-streamcore-schema-version" -eq "1.0") { Pass "S3 metadata: x-streamcore-schema-version = 1.0" }
    else { Fail "S3 metadata" "x-streamcore-schema-version missing or wrong: '$($meta.'x-streamcore-schema-version')'" }
} else {
    Fail "S3 object" "no object found containing eventId '$eventId'"
}

# =============================================================================
# 8. REJECTED EVENT: events/invalid/bad-currency.json -> INVALID_CURRENCY
#    -> SM SUCCEEDED -> SendToValidationDLQ -> MessageId in output
# =============================================================================
Write-Host "`n=== 8. Rejected event (invalid currency) ==="
awslocal sqs purge-queue --queue-url $validationDLQUrl | Out-Null
Start-Sleep -Seconds 1

# Read the invalid fixture; strip the _note key (harmless but cleaner on the wire)
$badObj = Get-Content (Join-Path $FixturesRoot "invalid\bad-currency.json") -Raw | ConvertFrom-Json
$badObj.PSObject.Properties.Remove("_note")
$badObj | Add-Member -NotePropertyName clientTimestamp `
    -NotePropertyValue ((Get-Date).ToUniversalTime().ToString("yyyy-MM-ddTHH:mm:ss.fffZ")) -Force

$s8Payload = @{
    body           = ($badObj | ConvertTo-Json -Depth 10 -Compress)
    headers        = @{ authorization = "Bearer $validToken" }
    requestContext = @{ authorizer = @{ tenantId = "tenant_test"; tenantRole = "sdk_writer" } }
} | ConvertTo-Json -Compress -Depth 10

$s8Resp    = Invoke-Lambda $ingestFn $s8Payload
$badIngest = $s8Resp.body | ConvertFrom-Json

$badIngestionId = $badIngest.ingestionId
Write-Host "    bad ingestionId: $badIngestionId"

Start-Sleep -Seconds 1
$shardIter2 = (awslocal kinesis get-shard-iterator `
    --stream-name streamcore-events-dev `
    --shard-id shardId-000000000000 `
    --shard-iterator-type TRIM_HORIZON `
    --output json | ConvertFrom-Json).ShardIterator
$allRecords2 = (awslocal kinesis get-records `
    --shard-iterator $shardIter2 `
    --output json | ConvertFrom-Json).Records
$badRecord = $allRecords2 | Where-Object {
    $data = [System.Text.Encoding]::UTF8.GetString([Convert]::FromBase64String($_.Data))
    ($data | ConvertFrom-Json).ingestionId -eq $badIngestionId
} | Select-Object -First 1

if (-not $badRecord) {
    Fail "bad event Kinesis record" "could not find record for ingestionId $badIngestionId"
} else {
    $badEnvelope = @{
        Records = @(@{
            kinesis = @{
                data = $badRecord.Data; partitionKey = $badRecord.PartitionKey
                sequenceNumber = $badRecord.SequenceNumber; kinesisSchemaVersion = "1.0"
                approximateArrivalTimestamp = 1718356800
            }
            eventSource = "aws:kinesis"; awsRegion = "eu-west-1"
            eventSourceARN = "arn:aws:kinesis:eu-west-1:000000000000:stream/streamcore-events-dev"
            eventID = "shardId-000000000000:$($badRecord.SequenceNumber)"
            eventName = "aws:kinesis:record"
            invokeIdentityArn = "arn:aws:iam::000000000000:role/lambda-role"
            eventVersion = "1.0"
        })
    } | ConvertTo-Json -Depth 10 -Compress

    $badFile1 = Join-Path $env:TEMP "sc_bad_event.json"
    $badFile2 = Join-Path $env:TEMP "sc_bad_response.json"
    $badEnvelope | Set-Content $badFile1 -Encoding ASCII
    awslocal lambda invoke --function-name $consumerFn --payload "file://$badFile1" --output json $badFile2 | Out-Null
    Remove-Item $badFile1, $badFile2 -ErrorAction SilentlyContinue

    Write-Host "    waiting for state machine..."
    Start-Sleep -Seconds 10

    $badExec = (awslocal stepfunctions list-executions `
        --state-machine-arn $smArn --output json | ConvertFrom-Json).executions |
        Where-Object { $_.name -eq $badIngestionId } | Select-Object -First 1

    if ($badExec -and $badExec.status -eq "SUCCEEDED") {
        Pass "rejected event: SM execution SUCCEEDED"

        $badOutput = (awslocal stepfunctions describe-execution `
            --execution-arn $badExec.executionArn `
            --output json | ConvertFrom-Json).output | ConvertFrom-Json

        if ($badOutput.MessageId) {
            Pass "rejected event routed to SendToValidationDLQ (MessageId: $($badOutput.MessageId))"
        } else {
            Fail "DLQ routing" "expected SQS MessageId in execution output, got: $($badOutput | ConvertTo-Json -Compress)"
        }
    } else {
        $badStatus = if ($badExec) { $badExec.status } else { "NOT FOUND" }
        Fail "bad execution" "expected SUCCEEDED, got '$badStatus'"
    }

    $dlqResp = awslocal sqs receive-message `
        --queue-url $validationDLQUrl `
        --max-number-of-messages 1 `
        --wait-time-seconds 0 `
        --output json | ConvertFrom-Json
    $dlqMsg = if ($dlqResp.PSObject.Properties['Messages']) { $dlqResp.Messages | Select-Object -First 1 } else { $null }

    if ($dlqMsg) {
        $dlqBody = $dlqMsg.Body | ConvertFrom-Json
        if ($dlqBody.ingestionId -eq $badIngestionId) {
            Pass "ValidationDLQ contains rejected event with correct ingestionId"
        } else {
            Fail "ValidationDLQ ingestionId" "expected $badIngestionId, got '$($dlqBody.ingestionId)'"
        }
    } else {
        Fail "ValidationDLQ" "no message received from queue"
    }

    $exprFile2 = Join-Path $env:TEMP "sc_expr2.json"
    @{ ':pk' = @{ S = 'tenant_test#order.placed' } } | ConvertTo-Json -Compress | Set-Content $exprFile2 -Encoding ASCII
    $allItems = (awslocal dynamodb query `
        --table-name streamcore-events-dev `
        --key-condition-expression "PK = :pk" `
        --expression-attribute-values "file://$exprFile2" `
        --output json | ConvertFrom-Json).Items
    Remove-Item $exprFile2
    $rejectedItem = $allItems | Where-Object { $_.ingestionId.S -eq $badIngestionId }
    if ($null -eq $rejectedItem) { Pass "rejected event absent from DynamoDB" }
    else { Fail "rejected event isolation" "item found in DynamoDB -- rejected events must not be persisted" }
}

# =============================================================================
# 9. IDEMPOTENCY: duplicate Kinesis delivery -> ExecutionAlreadyExists (STANDARD)
# =============================================================================
Write-Host "`n=== 9. Idempotency (duplicate Kinesis delivery) ==="
$dupFile1 = Join-Path $env:TEMP "sc_dup_event.json"
$dupFile2 = Join-Path $env:TEMP "sc_dup_response.json"
$kinesisEvent | Set-Content $dupFile1 -Encoding ASCII
$dupResult = awslocal lambda invoke `
    --function-name $consumerFn `
    --payload "file://$dupFile1" `
    --output json `
    $dupFile2 | ConvertFrom-Json
Remove-Item $dupFile1, $dupFile2 -ErrorAction SilentlyContinue

$dupError = if ($dupResult.PSObject.Properties['FunctionError']) { $dupResult.FunctionError } else { $null }
if ($dupResult.StatusCode -eq 200 -and -not $dupError) { Pass "Consumer Lambda re-invocation did not crash" }
else { Fail "idempotency re-invoke" "Lambda error: $dupError" }

Start-Sleep -Seconds 3

$exprFile3 = Join-Path $env:TEMP "sc_expr3.json"
@{ ':pk' = @{ S = 'tenant_test#order.placed' } } | ConvertTo-Json -Compress | Set-Content $exprFile3 -Encoding ASCII
$finalItems = (awslocal dynamodb query `
    --table-name streamcore-events-dev `
    --key-condition-expression "PK = :pk" `
    --expression-attribute-values "file://$exprFile3" `
    --output json | ConvertFrom-Json).Items
Remove-Item $exprFile3

$dedupItems = @($finalItems | Where-Object { $_.ingestionId.S -eq $ingestionId })
if ($dedupItems.Count -eq 1) { Pass "exactly 1 DynamoDB item after duplicate delivery (idempotent)" }
else { Fail "idempotency" "expected 1 item, found $($dedupItems.Count)" }

# =============================================================================
# 10. WRITER IDEMPOTENCY (direct double-invoke -- attribute_not_exists PK path)
#
#  Section 9 tests Consumer-level dedup (ExecutionAlreadyExists), STANDARD only.
#  On real AWS with EXPRESS executions, duplicates CAN reach the Writer.
#  Invoke WriterFunction twice with the same event (same PK+SK).
#  Second call hits ConditionalCheckFailedException; Writer suppresses it.
#  Assert: no Lambda error on either call; exactly 1 DynamoDB item.
# =============================================================================
Write-Host "`n=== 10. Writer idempotency (direct double-invoke) ==="
$idempTs = "2020-01-01T00:00:00.000Z"
$idempEvent = @{
    eventId           = "bb0e8400-e29b-41d4-a716-446655440099"
    eventType         = "user.login"
    schemaVersion     = "1.0"
    schemaVersionUsed = "1.0"
    clientTimestamp   = $idempTs
    ingestedAt        = $idempTs
    processedAt       = $idempTs
    tenantId          = "tenant_test"
    ingestionId       = "idemp-direct-001"
    pipelineVersion   = "2.0.0"
    payload           = @{ source = "web" }
} | ConvertTo-Json -Compress -Depth 5

$wf  = Join-Path $env:TEMP "sc_writer_idemp.json"
$wr1 = Join-Path $env:TEMP "sc_writer_resp1.json"
$wr2 = Join-Path $env:TEMP "sc_writer_resp2.json"
$idempEvent | Set-Content $wf -Encoding ASCII

awslocal lambda invoke --function-name $writerFn --payload "file://$wf" --output json $wr1 | Out-Null
$r1 = Get-Content $wr1 | ConvertFrom-Json
$r1err = if ($r1.PSObject.Properties['FunctionError']) { $r1.FunctionError } else { $null }
if (-not $r1err) { Pass "Writer first invocation: no error" }
else              { Fail "Writer first invocation" "FunctionError=$r1err" }

awslocal lambda invoke --function-name $writerFn --payload "file://$wf" --output json $wr2 | Out-Null
$r2 = Get-Content $wr2 | ConvertFrom-Json
$r2err = if ($r2.PSObject.Properties['FunctionError']) { $r2.FunctionError } else { $null }
if (-not $r2err) { Pass "Writer second invocation (duplicate): ConditionalCheckFailed suppressed, no error" }
else              { Fail "Writer second invocation" "FunctionError=$r2err" }

Remove-Item $wf, $wr1, $wr2 -ErrorAction SilentlyContinue

$idempKeyFile = Join-Path $env:TEMP "sc_idemp_key.json"
@{
    PK = @{ S = "tenant_test#user.login" }
    SK = @{ S = "${idempTs}#bb0e8400-e29b-41d4-a716-446655440099" }
} | ConvertTo-Json -Compress | Set-Content $idempKeyFile -Encoding ASCII

$idempItem = (awslocal dynamodb get-item `
    --table-name streamcore-events-dev `
    --key "file://$idempKeyFile" `
    --output json | ConvertFrom-Json).Item
Remove-Item $idempKeyFile -ErrorAction SilentlyContinue

if ($null -ne $idempItem -and $idempItem.eventId.S -eq "bb0e8400-e29b-41d4-a716-446655440099") {
    Pass "Writer idempotency: item exists at exact PK+SK after double-invoke (attribute_not_exists guard)"
} else {
    Fail "Writer idempotency" "item not found at PK=tenant_test#user.login SK=${idempTs}#bb0e8400..."
}

Write-Host ""
if ($failures -eq 0) { Write-Host "=== ALL TESTS PASSED ===" -ForegroundColor Green; exit 0 }
else                  { Write-Host "=== $failures TEST(S) FAILED ===" -ForegroundColor Red; exit $failures }
