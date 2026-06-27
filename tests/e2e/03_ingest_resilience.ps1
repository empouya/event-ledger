# tests/e2e/03_ingest_resilience.ps1  --  Area: Ingest resilience  (section 11)
. "$PSScriptRoot\_common.ps1"

# =============================================================================
# 11. INGEST 503 PATH (FR-STR-02)
#     Override EVENT_STREAM_NAME to nonexistent stream -> 503 STREAM_UNAVAILABLE.
#     try/finally guarantees the env var is restored even if the section fails.
# =============================================================================
Write-Host "`n=== 11. Ingest 503 path (bad stream name) ==="

$badEnvFile        = Join-Path $env:TEMP "sc_bad_env.json"
$goodEnvFile       = Join-Path $env:TEMP "sc_good_env.json"
$ingestPayloadFile = Join-Path $env:TEMP "sc_ingest_503.json"
$ingest503RespFile = Join-Path $env:TEMP "sc_ingest_503_resp.json"

# Use user.login fixture as the ingest body; override clientTimestamp to now.
$loginEvt = Get-Content (Join-Path $FixturesRoot "valid\user.login.json") -Raw | ConvertFrom-Json
$loginEvt | Add-Member -NotePropertyName clientTimestamp `
    -NotePropertyValue ((Get-Date).ToUniversalTime().ToString("yyyy-MM-ddTHH:mm:ss.fffZ")) -Force

@{
    body       = ($loginEvt | ConvertTo-Json -Depth 10 -Compress)
    headers    = @{ "authorization" = "Bearer $validToken" }
    httpMethod = "POST"
    path       = "/v1/events"
    requestContext = @{
        authorizer = @{ tenantId = "tenant_test"; tenantRole = "sdk_writer" }
    }
} | ConvertTo-Json -Compress -Depth 10 | Set-Content $ingestPayloadFile -Encoding ASCII

try {
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
} finally {
    @{ Variables = @{ EVENT_STREAM_NAME = $streamName; AWS_REGION = "eu-west-1" } } `
        | ConvertTo-Json -Compress | Set-Content $goodEnvFile -Encoding ASCII
    awslocal lambda update-function-configuration `
        --function-name $ingestFn `
        --environment "file://$goodEnvFile" | Out-Null
    Remove-Item $badEnvFile, $goodEnvFile, $ingestPayloadFile, $ingest503RespFile -ErrorAction SilentlyContinue
    Start-Sleep -Seconds 3
}

# Confirm IngestFunction is back to normal with a real request.
$restorePayload = @{
    body           = ($loginEvt | ConvertTo-Json -Depth 10 -Compress)
    headers        = @{}
    requestContext = @{ authorizer = @{ tenantId = "tenant_test"; tenantRole = "sdk_writer" } }
} | ConvertTo-Json -Compress -Depth 10

$restoreResp = Invoke-Lambda $ingestFn $restorePayload
if ($restoreResp.statusCode -eq 202) { Pass "Ingest restored: 202 accepted after stream name fix" }
else { Fail "Ingest restored" "expected 202, got $($restoreResp.statusCode)" }

Write-Host ""
if ($failures -eq 0) { Write-Host "=== ALL TESTS PASSED ===" -ForegroundColor Green; exit 0 }
else                  { Write-Host "=== $failures TEST(S) FAILED ===" -ForegroundColor Red; exit $failures }
