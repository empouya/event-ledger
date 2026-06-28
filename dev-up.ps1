# dev-up.ps1 -- bring the local StreamCore dev environment up in one command.
#
# DOT-SOURCE it so the env vars persist in your session:
#     . .\dev-up.ps1
#
# What it does, in order:
#   1. Put Python 3.12 on PATH (sam / cfn-lint need it).
#   2. Ensure an AWS login (real-AWS only; LocalStack uses dummy creds).
#   3. Validate -> build -> deploy the stack to LocalStack (create or update).
#   4. Set the session env vars the tests use.
#   5. Seed LocalStack configs (PII salts, JWT secret, TenantConfiguration, SES identity).
#      Events are NOT seeded here -- use scripts/seed-events.ps1 for that.

$ErrorActionPreference = "Stop"

# Run from the project root so template.yaml resolves regardless of caller location.
Set-Location $PSScriptRoot

# - 1. Python 3.12 on PATH -
# Detect via the Windows 'py' launcher so no machine-specific path is hardcoded.
$py312 = & py -3.12 -c "import sys, os; print(os.path.dirname(sys.executable))" 2>$null
if ($py312) {
    $env:PATH = "$py312;$env:PATH"
    Write-Host "[1/5] Python 3.12 on PATH: $py312"
} else {
    Write-Warning "[1/5] Python 3.12 not found via 'py -3.12'. Install it or add it to PATH manually."
}

# - 2. AWS login (only needed for real-AWS work) -
$env:AWS_PROFILE = "streamcore-dev"
$ident = $null
try { $ident = aws sts get-caller-identity --profile streamcore-dev 2>$null } catch {}
if (-not $ident) {
    Write-Host "[2/5] Not logged in to AWS. Running 'aws sso login' (skip with Ctrl-C if local-only)..."
    try { aws sso login --profile streamcore-dev } catch { Write-Warning "SSO login skipped/failed -- fine for LocalStack-only work." }
} else {
    Write-Host "[2/5] AWS identity OK (profile streamcore-dev)."
}

# - 3. Validate, build, deploy to LocalStack -
Write-Host "[3/5] Validating, building, and deploying to LocalStack..."
cfn-lint template.yaml
sam validate --lint
samlocal build
samlocal deploy --config-env local

# - 4. Session env vars -
# JWT secret for the local mock authorizer (must match streamcore/jwt-secret,
# seeded below). Well-known, LOCAL-ONLY value (ADR-009) -- never use on real AWS.
$env:JWT_SECRET_LOCAL = "streamcore-local-dev-jwt-secret-CHANGE-FOR-REAL-AWS"

# The REST API id changes on every LocalStack restart -- resolve it fresh.
$apiId = (awslocal apigateway get-rest-apis --query "items[0].id" --output text 2>$null)
if ($apiId -and $apiId -ne "None") {
    $env:LOCAL_BASE_URL = "http://localhost:4566/restapis/$apiId/dev/_user_request_"
    Write-Host "[4/5] LOCAL_BASE_URL = $env:LOCAL_BASE_URL"
} else {
    Write-Warning "[4/5] Could not resolve the LocalStack API Gateway id -- is the deploy healthy?"
}

# - 5. Seed configs (NOT events) -
Write-Host "[5/5] Seeding configs (PII salts, JWT secret, TenantConfiguration, SES identity)..."

# 5a. Per-tenant PII salts (CMK-encrypted). Idempotent: skip if present.
foreach ($tenant in @("tenant_test", "tenant_dev")) {
    $secretName = "streamcore/pii-salt/$tenant"
    $exists = (awslocal secretsmanager list-secrets --filters Key=name,Values=$secretName | ConvertFrom-Json).SecretList.Count
    if ($exists -gt 0) { Write-Host "  exists   $secretName"; continue }
    $salt = -join ((1..32) | ForEach-Object { "{0:x2}" -f (Get-Random -Max 256) })
    $tmp = "$env:TEMP\seed-$tenant.json"
    @{ piiSalt = $salt } | ConvertTo-Json -Compress | Set-Content $tmp -Encoding ASCII
    awslocal secretsmanager create-secret --name $secretName `
        --kms-key-id alias/streamcore-pipeline-key `
        --secret-string "file://$tmp" | Out-Null
    Remove-Item $tmp
    Write-Host "  created  $secretName"
}

# 5b. JWT signing secret for the mock authorizer. Idempotent.
$jwtName = "streamcore/jwt-secret"
if ((awslocal secretsmanager list-secrets --filters Key=name,Values=$jwtName | ConvertFrom-Json).SecretList.Count -gt 0) {
    Write-Host "  exists   $jwtName"
} else {
    $tmp = "$env:TEMP\seed-jwt.json"
    @{ jwtSecret = $env:JWT_SECRET_LOCAL } | ConvertTo-Json -Compress | Set-Content $tmp -Encoding ASCII
    awslocal secretsmanager create-secret --name $jwtName --secret-string "file://$tmp" | Out-Null
    Remove-Item $tmp
    Write-Host "  created  $jwtName"
}

# 5c. TenantConfiguration items. Idempotent via condition-expression.
#     tenant_test: activeEventTypes non-empty -> VAL-003 rejects other types.
#     tenant_dev:  empty allow-list -> all registered types permitted.
$configTable = "streamcore-tenant-config-dev"
$now = (Get-Date -Format "yyyy-MM-ddTHH:mm:ssZ")
$tenantConfigs = @(
    @{ tenantId = "tenant_test"; displayName = "Test Tenant"; reportRecipients = @("ops@streamcore.io"); activeEventTypes = @("order.placed", "user.login"); status = "active" },
    @{ tenantId = "tenant_dev";  displayName = "Dev Tenant";  reportRecipients = @();                    activeEventTypes = @();                          status = "active" }
)
foreach ($cfg in $tenantConfigs) {
    $tid = $cfg.tenantId
    $saltArn = awslocal secretsmanager describe-secret --secret-id "streamcore/pii-salt/$tid" --query "ARN" --output text 2>$null
    if (-not $saltArn) { $saltArn = "" }
    $recipL = ($cfg.reportRecipients | ForEach-Object { "{`"S`": `"$_`"}" }) -join ","
    $typesL = ($cfg.activeEventTypes  | ForEach-Object { "{`"S`": `"$_`"}" }) -join ","
    $tmp = "$env:TEMP\seed-tcfg-$tid.json"

    $json  = "{`"tenantId`":{`"S`":`"$tid`"},"
    $json += "`"displayName`":{`"S`":`"$($cfg.displayName)`"},"
    $json += "`"reportRecipients`":{`"L`":[$recipL]},"
    $json += "`"activeEventTypes`":{`"L`":[$typesL]},"
    $json += "`"status`":{`"S`":`"$($cfg.status)`"},"
    $json += "`"piiSaltSecretArn`":{`"S`":`"$saltArn`"},"
    $json += "`"createdAt`":{`"S`":`"$now`"},"
    $json += "`"updatedAt`":{`"S`":`"$now`"}}"
    $json | Set-Content $tmp -Encoding ASCII

    try {
        awslocal dynamodb put-item --table-name $configTable --item "file://$tmp" `
            --condition-expression "attribute_not_exists(tenantId)" | Out-Null
        Write-Host "  created  TenantConfiguration/$tid"
    } catch {
        Write-Host "  exists   TenantConfiguration/$tid"
    }
    Remove-Item $tmp
}

# 5d. SES sender identity.
awslocal ses verify-email-identity --email-address reports@streamcore.io | Out-Null
Write-Host "  verified reports@streamcore.io"

Write-Host "`nEnvironment is up. Run tests with the suite in tests/e2e/, or seed sample events with scripts/seed-events.ps1."
