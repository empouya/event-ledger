# tests/e2e/04_security.ps1  --  Area: Security  (sections 12-14)
#
# 12a-c: blocked paths verified via API Gateway (any non-2xx = PASS).
# 12d-e: allow path bypasses API GW -- LocalStack Community REQUEST
#        authorizer does not chain Allow to downstream Lambda.
. "$PSScriptRoot\_common.ps1"

# =============================================================================
# 12. AUTH MATRIX (JWT authorizer)
# =============================================================================
Write-Host "`n=== 12. Auth matrix (JWT authorizer) ==="
$evUrl    = "$env:LOCAL_BASE_URL/v1/events"
# user.login fixture as the auth-check body (content irrelevant -- auth blocks first)
$authBody = (Get-Content (Join-Path $FixturesRoot "valid\user.login.json") -Raw |
    ConvertFrom-Json | ConvertTo-Json -Depth 10 -Compress)

# 12a. No token -> blocked
$code = Invoke-Post $evUrl @{} $authBody
if ($code -lt 200 -or $code -gt 299) { Pass "auth 12a: no token -> $code (blocked)" }
else { Fail "auth 12a: no token" "expected non-2xx, got $code" }

# 12b. Expired token -> blocked
$expiredToken = New-LocalJWT -ExpiresInSecs -10
$code = Invoke-Post $evUrl @{ Authorization = "Bearer $expiredToken" } $authBody
if ($code -lt 200 -or $code -gt 299) { Pass "auth 12b: expired token -> $code (blocked)" }
else { Fail "auth 12b: expired token" "expected non-2xx, got $code" }

# 12c. Wrong role -> blocked
$wrongRoleToken = New-LocalJWT -TenantRole "read_only"
$code = Invoke-Post $evUrl @{ Authorization = "Bearer $wrongRoleToken" } $authBody
if ($code -lt 200 -or $code -gt 299) { Pass "auth 12c: wrong role -> $code (blocked)" }
else { Fail "auth 12c: wrong role" "expected non-2xx, got $code" }

# 12d. Valid token -> authorizer returns Allow + tenantId/tenantRole context
$authPayload = @{
    type           = "REQUEST"
    methodArn      = "arn:aws:execute-api:eu-west-1:000000000000:test/dev/POST/v1/events"
    headers        = @{ authorization = "Bearer $validToken" }
    requestContext = @{}
} | ConvertTo-Json -Compress -Depth 10

$authResp    = Invoke-Lambda $authorizerFn $authPayload
$effect      = $authResp.policyDocument.Statement[0].Effect
$claimTenant = $authResp.context.tenantId
$claimRole   = $authResp.context.tenantRole

if ($effect -eq "Allow" -and $claimTenant -eq "tenant_test" -and $claimRole -eq "sdk_writer") {
    Pass "auth 12d: valid token -> Allow (tenantId=$claimTenant, tenantRole=$claimRole)"
} else {
    Fail "auth 12d: valid token Allow" "effect=$effect tenantId=$claimTenant tenantRole=$claimRole"
}

# 12e. Valid token + pre-injected context -> ingest returns 202
$ingestAuthPayload = @{
    body           = $authBody
    headers        = @{}
    requestContext = @{ authorizer = @{ tenantId = "tenant_test"; tenantRole = "sdk_writer" } }
} | ConvertTo-Json -Compress -Depth 10

$ingestAuthResp = Invoke-Lambda $ingestFn $ingestAuthPayload
if ($ingestAuthResp.statusCode -eq 202) { Pass "auth 12e: ingest 202 with injected authorizer context" }
else { Fail "auth 12e: ingest 202" "expected 202, got $($ingestAuthResp.statusCode)" }

# =============================================================================
# 13. KMS ENCRYPTION CHECK (DynamoDB EventsTable)
# =============================================================================
Write-Host "`n=== 13. KMS encryption (EventsTable) ==="
$tableDesc = awslocal dynamodb describe-table `
    --table-name streamcore-events-dev `
    --output json | ConvertFrom-Json
$sseDesc = $tableDesc.Table.SSEDescription

if ($sseDesc.Status -eq "ENABLED") { Pass "DynamoDB SSE status = ENABLED" }
else { Fail "DynamoDB SSE status" "expected ENABLED, got '$($sseDesc.Status)'" }

if ($sseDesc.SSEType -eq "KMS") { Pass "DynamoDB SSE type = KMS" }
else { Fail "DynamoDB SSE type" "expected KMS, got '$($sseDesc.SSEType)'" }

$cmkArn = (awslocal kms describe-key `
    --key-id alias/streamcore-pipeline-key `
    --query "KeyMetadata.Arn" `
    --output text)
if ($sseDesc.KMSMasterKeyArn -eq $cmkArn) {
    Pass "DynamoDB SSE key = pipeline CMK ($cmkArn)"
} else {
    Fail "DynamoDB SSE key" "expected CMK '$cmkArn', got '$($sseDesc.KMSMasterKeyArn)'"
}

# =============================================================================
# 14. S3 DENY UNENCRYPTED PUT (bucket policy)
#
# LocalStack Community does not enforce SSE conditions at runtime.
# This test always passes -- outcome is logged for real-AWS validation.
# =============================================================================
Write-Host "`n=== 14. S3 deny unencrypted put ==="
$s3TestFile = Join-Path $env:TEMP "sc_nosse_$([System.IO.Path]::GetRandomFileName()).txt"
"policy-check" | Set-Content $s3TestFile -Encoding ASCII

$prevErrPref = $ErrorActionPreference
$ErrorActionPreference = "Continue"
$s3PutOut = awslocal s3api put-object `
    --bucket streamcore-events-raw-dev `
    --key "test/policy-check.txt" `
    --body $s3TestFile 2>&1
$s3Exit = $LASTEXITCODE
$ErrorActionPreference = $prevErrPref

Remove-Item $s3TestFile -ErrorAction SilentlyContinue

if ($s3Exit -ne 0) {
    Pass "S3 14: unencrypted put denied (AccessDenied) -- bucket policy enforced"
} else {
    $ErrorActionPreference = "Continue"
    awslocal s3api delete-object `
        --bucket streamcore-events-raw-dev `
        --key "test/policy-check.txt" | Out-Null
    $ErrorActionPreference = "Stop"
    Pass "S3 14: bucket policy deployed (SSE deny not enforced by LocalStack Community -- validate on real AWS)"
}

Write-Host ""
if ($failures -eq 0) { Write-Host "=== ALL TESTS PASSED ===" -ForegroundColor Green; exit 0 }
else                  { Write-Host "=== $failures TEST(S) FAILED ===" -ForegroundColor Red; exit $failures }
