# tests/e2e/test_e2e.ps1
#
# Repeatable end-to-end test for the Phase 2 pipeline on LocalStack.
#
# Prerequisites:
#   1. LocalStack running:      docker compose up -d
#   2. Stack deployed:          samlocal build && samlocal deploy --config-env local
#   3. PII salt exists:         run the secret-setup block below if missing
#   4. Env loaded:              . .\dev-env.ps1
#
# Secret setup (run once per docker compose up):
#   awslocal secretsmanager create-secret `
#       --name streamcore/pii-salt/tenant_test `
#       --secret-string '{"piiSalt":"0000000000000000000000000000000000000000000000000000000000000001"}'
#
# NOTE — LocalStack Community ESM limitation:
#   The Kinesis->Lambda ESM polls once after deployment, then the shard iterator
#   expires. This test bypasses it by invoking the Consumer Lambda directly.
#   We DO read the real Kinesis record via get-records (tests the Ingest->Consumer
#   wire-format contract). The ESM trigger itself is validated in the real-AWS window.
#
# Usage:  .\tests\e2e\test_e2e.ps1
# Exit 0 = all pass   Exit 1 = one or more failures

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"
$failures = 0

function Pass { param($label) Write-Host "[PASS] $label" -ForegroundColor Green }
function Fail { param($label, $detail) Write-Host "[FAIL] $label -- $detail" -ForegroundColor Red; $script:failures++ }

# ── Resource lookup ───────────────────────────────────────────────────────────
$consumerFn = (awslocal cloudformation describe-stack-resource `
    --stack-name streamcore-local `
    --logical-resource-id ConsumerFunction `
    --output json | ConvertFrom-Json).StackResourceDetail.PhysicalResourceId

$smArn = (awslocal cloudformation describe-stacks `
    --stack-name streamcore-local `
    --output json | ConvertFrom-Json).Stacks[0].Outputs |
    Where-Object { $_.OutputKey -eq "ProcessingStateMachineArn" } |
    Select-Object -ExpandProperty OutputValue

Write-Host "Consumer: $consumerFn"
Write-Host "State machine: $smArn"

# ═══════════════════════════════════════════════════════════════════════════════
# 1. HEALTH CHECK
# ═══════════════════════════════════════════════════════════════════════════════
Write-Host "`n=== 1. /health ==="
$health = Invoke-RestMethod -Method Get -Uri "$env:LOCAL_BASE_URL/health"

if ($health.status -eq "healthy")     { Pass "status = healthy" }
else                                   { Fail "status" "expected 'healthy', got '$($health.status)'" }

if ($health.checks.dynamodb -eq "ok") { Pass "dynamodb check = ok" }
else                                   { Fail "dynamodb check" "expected 'ok', got '$($health.checks.dynamodb)'" }

# ═══════════════════════════════════════════════════════════════════════════════
# 2. POST VALID EVENT (dynamic clientTimestamp avoids TIMESTAMP_TOO_OLD rejection)
# ═══════════════════════════════════════════════════════════════════════════════
Write-Host "`n=== 2. POST /v1/events (valid) ==="
$eventObj = Get-Content "events\ingest-order-placed.json" -Raw | ConvertFrom-Json
$eventObj | Add-Member -NotePropertyName clientTimestamp `
    -NotePropertyValue ((Get-Date).ToUniversalTime().ToString("yyyy-MM-ddTHH:mm:ss.fffZ")) -Force

$ingest = Invoke-RestMethod -Method Post `
    -Uri "$env:LOCAL_BASE_URL/v1/events" `
    -Headers @{ "Content-Type" = "application/json"; "X-Tenant-Id" = "tenant_test" } `
    -Body ($eventObj | ConvertTo-Json -Depth 10 -Compress)

if ($ingest.status -eq "accepted") { Pass "response status = accepted" }
else                                { Fail "response status" $ingest.status }

$ingestionId = $ingest.ingestionId
$ingestedAt  = $ingest.timestamp
$eventId     = $eventObj.eventId
Write-Host "    ingestionId: $ingestionId"
Write-Host "    ingestedAt:  $ingestedAt"
Write-Host "    eventId:     $eventId"

# ═══════════════════════════════════════════════════════════════════════════════
# 3. READ REAL KINESIS RECORD
#    The Ingest Lambda writes to Kinesis synchronously — the record is readable
#    immediately after the POST returns. TRIM_HORIZON gets all records; we filter
#    by ingestionId so previous test-run records don't interfere.
# ═══════════════════════════════════════════════════════════════════════════════
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

# ═══════════════════════════════════════════════════════════════════════════════
# 4. INVOKE CONSUMER LAMBDA WITH THE REAL RECORD
# ═══════════════════════════════════════════════════════════════════════════════
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

# Wait for the 4-step state machine (Validator->PiiExtractor->Enricher->Writer).
Write-Host "    waiting for state machine..."
Start-Sleep -Seconds 10

# ═══════════════════════════════════════════════════════════════════════════════
# 5. STATE MACHINE: ASSERT SUCCEEDED
# ═══════════════════════════════════════════════════════════════════════════════
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

# ═══════════════════════════════════════════════════════════════════════════════
# 6. DYNAMODB ASSERTIONS
#    - item found  - SK = ingestedAt#eventId (not processedAt — the Phase 1 bug)
#    - status, pipelineVersion, schemaVersionUsed, expiresAt
#    - PII: userId must be a 64-char hex digest
# ═══════════════════════════════════════════════════════════════════════════════
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
    # SK must start with ingestedAt (not processedAt — that changed on every retry)
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

    # PII: userId in payload must be a 64-char lowercase hex string, not the raw value
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

# ═══════════════════════════════════════════════════════════════════════════════
# 7. S3 ASSERTION
#    Key pattern: {tenantId}/{eventType}/{YYYY-MM-DD}/{eventId}.json
# ═══════════════════════════════════════════════════════════════════════════════
Write-Host "`n=== 7. S3 assertion ==="
$s3Objects = (awslocal s3api list-objects `
    --bucket streamcore-events-raw-dev `
    --output json | ConvertFrom-Json).Contents

$s3Match = $s3Objects | Where-Object { $_.Key -like "*$eventId*" } | Select-Object -First 1

if ($s3Match) { Pass "S3 object exists: $($s3Match.Key)" }
else           { Fail "S3 object" "no object found containing eventId '$eventId'" }

# ═══════════════════════════════════════════════════════════════════════════════
# 8. REJECTED EVENT: bad currency -> SM SUCCEEDED with status=rejected, absent from DynamoDB
# ═══════════════════════════════════════════════════════════════════════════════
Write-Host "`n=== 8. Rejected event (invalid currency) ==="
$badObj = Get-Content "events\ingest-order-placed.json" -Raw | ConvertFrom-Json
$badObj | Add-Member -NotePropertyName clientTimestamp `
    -NotePropertyValue ((Get-Date).ToUniversalTime().ToString("yyyy-MM-ddTHH:mm:ss.fffZ")) -Force
# Use a distinct eventId so this event is unambiguous in S3/DynamoDB checks.
$badObj | Add-Member -NotePropertyName eventId `
    -NotePropertyValue "660e8400-e29b-41d4-a716-446655440002" -Force
$badObj.payload.currency = "XYZ"   # not in ACCEPTED_CURRENCIES -> Validator rejects

$badIngest = Invoke-RestMethod -Method Post `
    -Uri "$env:LOCAL_BASE_URL/v1/events" `
    -Headers @{ "Content-Type" = "application/json"; "X-Tenant-Id" = "tenant_test" } `
    -Body ($badObj | ConvertTo-Json -Depth 10 -Compress)

$badIngestionId = $badIngest.ingestionId
Write-Host "    bad ingestionId: $badIngestionId"

# Read the real Kinesis record for the bad event.
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
        $badOutput = (awslocal stepfunctions describe-execution `
            --execution-arn $badExec.executionArn `
            --output json | ConvertFrom-Json).output | ConvertFrom-Json
        if ($badOutput.status -eq "rejected") { Pass "invalid event routed to EventRejected state" }
        else { Fail "rejected routing" "expected status=rejected, got '$($badOutput.status)'" }
    } else {
        $badStatus = if ($badExec) { $badExec.status } else { "NOT FOUND" }
        Fail "bad execution" "expected SUCCEEDED, got '$badStatus'"
    }

    # Rejected events never reach the Writer — must be absent from DynamoDB.
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

# ═══════════════════════════════════════════════════════════════════════════════
# 9. IDEMPOTENCY: duplicate delivery yields exactly one DynamoDB item
#    Re-invoke Consumer with the same real Kinesis record (same ingestionId).
#    Consumer catches ExecutionAlreadyExists and skips — state machine doesn't
#    re-run, Writer doesn't re-fire, DynamoDB still has exactly 1 item.
# ═══════════════════════════════════════════════════════════════════════════════
Write-Host "`n=== 9. Idempotency (duplicate Kinesis delivery) ==="
$dupFile1 = Join-Path $env:TEMP "sc_dup_event.json"
$dupFile2 = Join-Path $env:TEMP "sc_dup_response.json"
$kinesisEvent | Set-Content $dupFile1 -Encoding ASCII     # same envelope as section 4
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

# ═══════════════════════════════════════════════════════════════════════════════
# SUMMARY
# ═══════════════════════════════════════════════════════════════════════════════
Write-Host ""
if ($failures -eq 0) {
    Write-Host "=== ALL TESTS PASSED ===" -ForegroundColor Green
    exit 0
} else {
    Write-Host "=== $failures TEST(S) FAILED ===" -ForegroundColor Red
    exit 1
}
