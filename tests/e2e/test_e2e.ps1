# tests/e2e/test_e2e.ps1
#
# Repeatable end-to-end test for the walking skeleton on LocalStack.
#
# Prerequisites:
#   1. LocalStack running:   docker compose up -d
#   2. Stack deployed:       samlocal build && samlocal deploy --config-env local
#   3. Env loaded:           . .\dev-env.ps1
#
# NOTE -- LocalStack Community limitation:
#   The Kinesis -> Lambda event source mapping (ESM) polls once after deployment,
#   then the shard iterator expires and LocalStack does not re-poll reliably.
#   This test bypasses the ESM and invokes the Consumer Lambda directly, which
#   exercises the same code path (base64 decode -> enrich -> DynamoDB write).
#   On real AWS the ESM polls continuously -- this workaround is LocalStack-only.
#
# Usage:  .\tests\e2e\test_e2e.ps1
# Exit 0 = all pass   Exit 1 = one or more failures

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"
$failures = 0

function Pass { param($label) Write-Host "[PASS] $label" -ForegroundColor Green }
function Fail { param($label, $detail) Write-Host "[FAIL] $label -- $detail" -ForegroundColor Red; $script:failures++ }

# ── 1. Health check ──────────────────────────────────────────────────────────
Write-Host "`n=== 1. /health ==="
$health = Invoke-RestMethod -Method Get -Uri "$env:LOCAL_BASE_URL/health"

if ($health.status -eq "healthy")         { Pass "status = healthy" }
else                                       { Fail "status" "expected 'healthy', got '$($health.status)'" }

if ($health.checks.dynamodb -eq "ok")     { Pass "dynamodb check = ok" }
else                                       { Fail "dynamodb check" "expected 'ok', got '$($health.checks.dynamodb)'" }

# ── 2. POST an event ─────────────────────────────────────────────────────────
Write-Host "`n=== 2. POST /v1/events ==="
$payload = Get-Content "events\ingest-order-placed.json" -Raw
$ingest  = Invoke-RestMethod -Method Post `
    -Uri "$env:LOCAL_BASE_URL/v1/events" `
    -Headers @{ "Content-Type" = "application/json"; "X-Tenant-Id" = "tenant_test" } `
    -Body $payload

if ($ingest.status -eq "accepted")        { Pass "response status = accepted" }
else                                       { Fail "response status" $ingest.status }

$ingestionId = $ingest.ingestionId
Write-Host "    ingestionId: $ingestionId"

# ── 3. Invoke Consumer Lambda directly ───────────────────────────────────────
# LocalStack Community's Kinesis ESM poller does not reliably restart after the
# first batch post-deploy. We invoke the Consumer Lambda with a synthetic Kinesis
# envelope -- the same code path the ESM uses, so the assertions are identical.
Write-Host "`n=== 3. Invoking Consumer Lambda directly ==="

# The physical function name has a SAM-generated suffix; look it up from CloudFormation
# rather than hardcoding it so the script survives redeployments.
$consumerFn = (awslocal cloudformation describe-stack-resource `
    --stack-name streamcore-local `
    --logical-resource-id ConsumerFunction `
    --output json | ConvertFrom-Json).StackResourceDetail.PhysicalResourceId
Write-Host "    function: $consumerFn"

# Reconstruct the payload the Ingest Lambda wrote to Kinesis for this request.
# Kinesis delivers record data as base64-encoded bytes; we encode it the same way.
$kinesisPayload = (@{
    eventType   = "order.placed"
    orderId     = "ORD-001"
    tenantId    = "tenant_test"
    ingestionId = $ingestionId
    ingestedAt  = $ingest.timestamp
} | ConvertTo-Json -Compress)

$encodedData = [Convert]::ToBase64String(
    [System.Text.Encoding]::UTF8.GetBytes($kinesisPayload))

# Wrap in the Kinesis event envelope -- the exact structure Lambda receives from the ESM.
$syntheticEvent = (@{
    Records = @(@{
        kinesis = @{
            data                        = $encodedData
            partitionKey                = "tenant_test"
            sequenceNumber              = "49590338271490256608559692540925702759324208523137515522"
            approximateArrivalTimestamp = 1718356800
            kinesisSchemaVersion        = "1.0"
        }
        eventSource       = "aws:kinesis"
        eventSourceARN    = "arn:aws:kinesis:eu-west-1:000000000000:stream/streamcore-events-dev"
        awsRegion         = "eu-west-1"
        eventID           = "shardId-000000000000:49590338271490256608559692540925702759324208523137515522"
        eventName         = "aws:kinesis:record"
        invokeIdentityArn = "arn:aws:iam::000000000000:role/lambda-role"
        eventVersion      = "1.0"
    })
} | ConvertTo-Json -Depth 10 -Compress)

# PowerShell strips quotes from JSON passed directly to external commands.
# Writing to a file and using file:// avoids this for both the event and
# the expression-attribute-values further down.
$eventFile    = Join-Path $env:TEMP "sc_kinesis_event.json"
$responseFile = Join-Path $env:TEMP "sc_consumer_response.json"
$syntheticEvent | Set-Content -Path $eventFile -Encoding ASCII

$invokeResult = awslocal lambda invoke `
    --function-name $consumerFn `
    --payload "file://$eventFile" `
    --output json `
    $responseFile | ConvertFrom-Json

Remove-Item $eventFile, $responseFile -ErrorAction SilentlyContinue

# FunctionError is absent on success. Accessing a missing property directly
# throws under Set-StrictMode, so we check for its existence first.
$funcError = if ($invokeResult.PSObject.Properties['FunctionError']) { $invokeResult.FunctionError } else { $null }
if ($invokeResult.StatusCode -eq 200 -and -not $funcError) {
    Pass "Consumer Lambda invoked successfully"
} else {
    Fail "Consumer Lambda invocation" "StatusCode=$($invokeResult.StatusCode) FunctionError=$funcError"
}

# The Consumer Lambda writes to DynamoDB synchronously; a short pause lets the
# write complete before we query.
Start-Sleep -Seconds 2

# ── 4. Assertions ─────────────────────────────────────────────────────────────
Write-Host "`n=== 4. Assertions ==="
$exprFile = Join-Path $env:TEMP "streamcore_expr.json"
'{ ":pk": {"S": "tenant_test#order.placed"} }' | Set-Content -Path $exprFile -Encoding ASCII
$queryResult = awslocal dynamodb query `
    --table-name "streamcore-events-dev" `
    --key-condition-expression "PK = :pk" `
    --expression-attribute-values "file://$exprFile" `
    --output json | ConvertFrom-Json
Remove-Item $exprFile

$item = $queryResult.Items | Where-Object { $_.ingestionId.S -eq $ingestionId }

if ($null -ne $item) { Pass "item found for ingestionId $ingestionId" }
else                  { Fail "item lookup" "item not in DynamoDB after direct Consumer invocation" }

if ($null -ne $item) {
    if ($item.status.S -eq "processed")    { Pass "status = processed" }
    else                                    { Fail "status" "expected 'processed', got '$($item.status.S)'" }

    if ($item.GSI1PK.S -eq "tenant_test")  { Pass "GSI1PK = tenant_test" }
    else                                    { Fail "GSI1PK" $item.GSI1PK.S }

    if ($item.expiresAt.N -gt 0)           { Pass "expiresAt is a non-zero integer (TTL set)" }
    else                                    { Fail "expiresAt" "expected a Unix epoch integer" }
}

# ── Summary ───────────────────────────────────────────────────────────────────
Write-Host ""
if ($failures -eq 0) {
    Write-Host "=== ALL TESTS PASSED ===" -ForegroundColor Green
    exit 0
} else {
    Write-Host "=== $failures TEST(S) FAILED ===" -ForegroundColor Red
    exit 1
}
