# tests/e2e/test_e2e.ps1
#
# Repeatable end-to-end test for the Phase 2-3 pipeline on LocalStack.
#
# Phase 3 additions:
#   Section 7  -- S3 key format (y/m/d/h) + x-streamcore-* object metadata
#   Section 8  -- rejected event routes to ValidationDLQ; DLQ message asserted
#   Section 10 -- Writer idempotency: direct double-invoke exercises
#                 attribute_not_exists(PK) conditional write path
#   Section 11 -- Ingest 503 path: bad stream name -> STREAM_UNAVAILABLE
#
# Prerequisites:
#   1. LocalStack running:      docker compose up -d
#   2. Stack deployed:          samlocal build && samlocal deploy --config-env local
#   3. PII salt exists:         ./scripts/seed-localstack.ps1
#   4. Env loaded:              . .\dev-env.ps1
#
# NOTE -- LocalStack Community ESM limitation:
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


# New-LocalJWT: mint a test HS256 JWT for local use.
# The secret must match streamcore/jwt-secret in Secrets Manager (seeded by
# seed-localstack.ps1).  Uses JWT_SECRET_LOCAL from dev-env.ps1.
function New-LocalJWT {
    param(
        [string]$TenantId   = "tenant_test",
        [string]$TenantRole = "sdk_writer",
        [int]$ExpiresInSecs = 3600
    )
    function ConvertTo-Base64Url([string]$s) {
        [Convert]::ToBase64String([Text.Encoding]::UTF8.GetBytes($s)) `
            -replace '=+$','' -replace '\+','-' -replace '/','_'
    }
    $header  = ConvertTo-Base64Url '{"alg":"HS256","typ":"JWT"}'
    $now     = [int]([DateTimeOffset]::UtcNow.ToUnixTimeSeconds())
    $payload = ConvertTo-Base64Url (@{
        iss        = "streamcore-local"
        sub        = $TenantId
        tenantId   = $TenantId
        tenantRole = $TenantRole
        iat        = $now
        exp        = $now + $ExpiresInSecs
    } | ConvertTo-Json -Compress)
    $sigInput = "$header.$payload"
    $keyBytes = [Text.Encoding]::UTF8.GetBytes($env:JWT_SECRET_LOCAL)
    $hmacObj  = [Security.Cryptography.HMACSHA256]::new($keyBytes)
    $sig      = [Convert]::ToBase64String(
        $hmacObj.ComputeHash([Text.Encoding]::UTF8.GetBytes($sigInput))
    ) -replace '=+$','' -replace '\+','-' -replace '/','_'
    return "$header.$payload.$sig"
}

# Resource lookup
$consumerFn = (awslocal cloudformation describe-stack-resource `
    --stack-name streamcore-local `
    --logical-resource-id ConsumerFunction `
    --output json | ConvertFrom-Json).StackResourceDetail.PhysicalResourceId

$writerFn = (awslocal cloudformation describe-stack-resource `
    --stack-name streamcore-local `
    --logical-resource-id WriterFunction `
    --output json | ConvertFrom-Json).StackResourceDetail.PhysicalResourceId

$ingestFn = (awslocal cloudformation describe-stack-resource `
    --stack-name streamcore-local `
    --logical-resource-id IngestFunction `
    --output json | ConvertFrom-Json).StackResourceDetail.PhysicalResourceId

$streamName = (awslocal cloudformation describe-stack-resource `
    --stack-name streamcore-local `
    --logical-resource-id EventStream `
    --output json | ConvertFrom-Json).StackResourceDetail.PhysicalResourceId

$stackOutputs = (awslocal cloudformation describe-stacks `
    --stack-name streamcore-local `
    --output json | ConvertFrom-Json).Stacks[0].Outputs

$smArn = ($stackOutputs |
    Where-Object { $_.OutputKey -eq "ProcessingStateMachineArn" }).OutputValue

$validationDLQUrl = ($stackOutputs |
    Where-Object { $_.OutputKey -eq "ValidationDLQUrl" }).OutputValue

Write-Host "Consumer:       $consumerFn"
Write-Host "Writer:         $writerFn"
Write-Host "Ingest:         $ingestFn"
Write-Host "State machine:  $smArn"

$validToken = New-LocalJWT -TenantId "tenant_test"
Write-Host "validToken minted for tenant_test"
Write-Host "ValidationDLQ:  $validationDLQUrl"

# =============================================================================
# 1. HEALTH CHECK
# =============================================================================
Write-Host "`n=== 1. /health ==="
$health = Invoke-RestMethod -Method Get -Uri "$env:LOCAL_BASE_URL/health"

if ($health.status -eq "healthy")     { Pass "status = healthy" }
else                                   { Fail "status" "expected 'healthy', got '$($health.status)'" }

if ($health.checks.dynamodb -eq "ok") { Pass "dynamodb check = ok" }
else                                   { Fail "dynamodb check" "expected 'ok', got '$($health.checks.dynamodb)'" }

# =============================================================================
# 2. POST VALID EVENT (dynamic clientTimestamp avoids TIMESTAMP_TOO_OLD rejection)
# =============================================================================
Write-Host "`n=== 2. POST /v1/events (valid) ==="
$eventObj = Get-Content "events\ingest-order-placed.json" -Raw | ConvertFrom-Json
$eventObj | Add-Member -NotePropertyName clientTimestamp `
    -NotePropertyValue ((Get-Date).ToUniversalTime().ToString("yyyy-MM-ddTHH:mm:ss.fffZ")) -Force

$ingest = Invoke-RestMethod -Method Post `
    -Uri "$env:LOCAL_BASE_URL/v1/events" `
    -Headers @{ "Content-Type" = "application/json"; "Authorization" = "Bearer $validToken" } `
    -Body ($eventObj | ConvertTo-Json -Depth 10 -Compress)

if ($ingest.status -eq "accepted") { Pass "response status = accepted" }
else                                { Fail "response status" $ingest.status }

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

# Wait for the 4-step state machine (Validator->PiiExtractor->Enricher->Writer).
Write-Host "    waiting for state machine..."
Start-Sleep -Seconds 10

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
# 8. REJECTED EVENT: bad currency -> ValidationError -> SendToValidationDLQ
#    Phase 3: SM ends SUCCEEDED; output is SQS MessageId, not {status:rejected}.
# =============================================================================
Write-Host "`n=== 8. Rejected event (invalid currency) ==="
$badObj = Get-Content "events\ingest-order-placed.json" -Raw | ConvertFrom-Json
$badObj | Add-Member -NotePropertyName clientTimestamp `
    -NotePropertyValue ((Get-Date).ToUniversalTime().ToString("yyyy-MM-ddTHH:mm:ss.fffZ")) -Force
$badObj | Add-Member -NotePropertyName eventId `
    -NotePropertyValue "660e8400-e29b-41d4-a716-446655440002" -Force
$badObj.payload.currency = "XYZ"

$badIngest = Invoke-RestMethod -Method Post `
    -Uri "$env:LOCAL_BASE_URL/v1/events" `
    -Headers @{ "Content-Type" = "application/json"; "Authorization" = "Bearer $validToken" } `
    -Body ($badObj | ConvertTo-Json -Depth 10 -Compress)

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
#     Section 9 tests Consumer-level dedup (ExecutionAlreadyExists), STANDARD only.
#     On real AWS with EXPRESS executions, duplicates CAN reach the Writer.
#     Invoke WriterFunction twice with the same event (same PK+SK).
#     Second call hits ConditionalCheckFailedException; Writer suppresses it silently.
#     Assert: no Lambda error on either call; exactly 1 DynamoDB item.
# =============================================================================
Write-Host "`n=== 10. Writer idempotency (direct double-invoke) ==="
$idempTs = (Get-Date).ToUniversalTime().ToString("yyyy-MM-ddTHH:mm:ss.fffZ")
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

$exprFile4 = Join-Path $env:TEMP "sc_expr4.json"
@{ ':pk' = @{ S = 'tenant_test#user.login' } } | ConvertTo-Json -Compress | Set-Content $exprFile4 -Encoding ASCII
$writerItems = (awslocal dynamodb query `
    --table-name streamcore-events-dev `
    --key-condition-expression "PK = :pk" `
    --expression-attribute-values "file://$exprFile4" `
    --output json | ConvertFrom-Json).Items
Remove-Item $exprFile4

$dedupWriterItems = @($writerItems | Where-Object { $_.eventId.S -eq "bb0e8400-e29b-41d4-a716-446655440099" })
if ($dedupWriterItems.Count -eq 1) { Pass "exactly 1 DynamoDB item after Writer duplicate (attribute_not_exists guard)" }
else { Fail "Writer idempotency" "expected 1 item, found $($dedupWriterItems.Count)" }

# =============================================================================
# 11. INGEST 503 PATH (FR-STR-02)
#     Override EVENT_STREAM_NAME to nonexistent stream -> 503 STREAM_UNAVAILABLE.
#     Restore correct stream name after test.
# =============================================================================
Write-Host "`n=== 11. Ingest 503 path (bad stream name) ==="

$badEnvFile        = Join-Path $env:TEMP "sc_bad_env.json"
$goodEnvFile       = Join-Path $env:TEMP "sc_good_env.json"
$ingestPayloadFile = Join-Path $env:TEMP "sc_ingest_503.json"
$ingest503RespFile = Join-Path $env:TEMP "sc_ingest_503_resp.json"

@{
    body    = (@{
        eventType       = "user.login"
        schemaVersion   = "1.0"
        clientTimestamp = (Get-Date).ToUniversalTime().ToString("yyyy-MM-ddTHH:mm:ss.fffZ")
        eventId         = "cc0e8400-e29b-41d4-a716-446655440999"
        payload         = @{ source = "web" }
    } | ConvertTo-Json -Compress)
    headers    = @{ "authorization" = "Bearer $validToken" }
    httpMethod = "POST"
    path       = "/v1/events"
    requestContext = @{
        authorizer = @{
            tenantId   = "tenant_test"
            tenantRole = "sdk_writer"
        }
    }
} | ConvertTo-Json -Compress | Set-Content $ingestPayloadFile -Encoding ASCII

@{ Variables = @{ EVENT_STREAM_NAME = "nonexistent-stream-xyz"; AWS_REGION = "eu-west-1" } } `
    | ConvertTo-Json -Compress | Set-Content $badEnvFile -Encoding ASCII
awslocal lambda update-function-configuration `
    --function-name $ingestFn `
    --environment "file://$badEnvFile" | Out-Null
Start-Sleep -Seconds 3

awslocal lambda invoke `
    --function-name $ingestFn `
    --payload "file://$ingestPayloadFile" `
    --output json `
    $ingest503RespFile | Out-Null
$resp503 = Get-Content $ingest503RespFile | ConvertFrom-Json
$body503 = $resp503.body | ConvertFrom-Json

if ($resp503.statusCode -eq 503) { Pass "Ingest 503: statusCode = 503" }
else { Fail "Ingest 503: statusCode" "expected 503, got $($resp503.statusCode)" }

if ($body503.error -eq "STREAM_UNAVAILABLE") { Pass "Ingest 503: error = STREAM_UNAVAILABLE" }
else { Fail "Ingest 503: error code" "expected STREAM_UNAVAILABLE, got '$($body503.error)'" }

@{ Variables = @{ EVENT_STREAM_NAME = $streamName; AWS_REGION = "eu-west-1" } } `
    | ConvertTo-Json -Compress | Set-Content $goodEnvFile -Encoding ASCII
awslocal lambda update-function-configuration `
    --function-name $ingestFn `
    --environment "file://$goodEnvFile" | Out-Null
Remove-Item $badEnvFile, $goodEnvFile, $ingestPayloadFile, $ingest503RespFile -ErrorAction SilentlyContinue
Start-Sleep -Seconds 3

$restoreResp = Invoke-RestMethod -Method Post `
    -Uri "$env:LOCAL_BASE_URL/v1/events" `
    -Headers @{ "Content-Type" = "application/json"; "Authorization" = "Bearer $validToken" } `
    -Body (@{
        eventType       = "user.login"
        schemaVersion   = "1.0"
        eventId         = "dd0e8400-e29b-41d4-a716-446655440999"
        clientTimestamp = (Get-Date).ToUniversalTime().ToString("yyyy-MM-ddTHH:mm:ss.fffZ")
        payload         = @{ source = "web" }
    } | ConvertTo-Json -Compress)
if ($restoreResp.status -eq "accepted") { Pass "Ingest restored: 202 accepted after stream name fix" }
else { Fail "Ingest restored" "expected accepted, got '$($restoreResp.status)'" }

# =============================================================================
# SUMMARY
# =============================================================================
Write-Host ""
if ($failures -eq 0) {
    Write-Host "=== ALL TESTS PASSED ===" -ForegroundColor Green
    exit 0
} else {
    Write-Host "=== $failures TEST(S) FAILED ===" -ForegroundColor Red
    exit 1
}
