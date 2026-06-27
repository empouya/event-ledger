#!/usr/bin/env pwsh
# scripts/seed-localstack.ps1
#
# Seeds LocalStack Secrets Manager with per-tenant PII salts and the
# JWT signing secret required by AuthorizerFunction.
# Run once after every `docker compose up` before deploying or running tests.
#
# Prerequisites: . .\dev-env.ps1 (sets JWT_SECRET_LOCAL)
# Idempotent: secrets that already exist are skipped, not overwritten.

$ErrorActionPreference = "Stop"

# ---- PII salts (one per tenant) ------------------------------------------
# Encrypted with the pipeline CMK (SEC-ENC-02 / SEC-SECRETS-01).
# PiiExtractorFunction needs kms:Decrypt on the CMK to read these.
# On real AWS, use the CMK ARN or alias; the alias is stable across deploys.
$tenants = @("tenant_test", "tenant_dev")

foreach ($tenant in $tenants) {
    $secretName = "streamcore/pii-salt/$tenant"

    $existing = awslocal secretsmanager list-secrets `
        --filters Key=name,Values=$secretName | ConvertFrom-Json

    if ($existing.SecretList.Count -gt 0) {
        Write-Host "EXISTS  $secretName"
        continue
    }

    $salt = -join ((1..32) | ForEach-Object { "{0:x2}" -f (Get-Random -Max 256) })
    @{ piiSalt = $salt } | ConvertTo-Json -Compress `
        | Set-Content "$env:TEMP\seed-$tenant.json" -Encoding ASCII

    awslocal secretsmanager create-secret `
        --name $secretName `
        --kms-key-id alias/streamcore-pipeline-key `
        --secret-string "file://$env:TEMP\seed-$tenant.json" | Out-Null

    Remove-Item "$env:TEMP\seed-$tenant.json"
    Write-Host "CREATED $secretName"
}

# ---- JWT signing secret for the local mock authorizer --------------------
# jwtSecret must match JWT_SECRET_LOCAL in dev-env.ps1.
# Value is intentionally well-known for local use only (ADR-009).
# Not encrypted with the CMK -- jwt-secret uses the default key.
$jwtSecretName = "streamcore/jwt-secret"

$jwtExisting = awslocal secretsmanager list-secrets `
    --filters Key=name,Values=$jwtSecretName | ConvertFrom-Json

if ($jwtExisting.SecretList.Count -gt 0) {
    Write-Host "EXISTS  $jwtSecretName"
} else {
    $jwtVal = if ($env:JWT_SECRET_LOCAL) { $env:JWT_SECRET_LOCAL } `
              else { "streamcore-local-dev-jwt-secret-CHANGE-FOR-REAL-AWS" }
    @{ jwtSecret = $jwtVal } | ConvertTo-Json -Compress `
        | Set-Content "$env:TEMP\seed-jwt.json" -Encoding ASCII

    awslocal secretsmanager create-secret `
        --name $jwtSecretName `
        --secret-string "file://$env:TEMP\seed-jwt.json" | Out-Null

    Remove-Item "$env:TEMP\seed-jwt.json"
    Write-Host "CREATED $jwtSecretName"
}


# ---- TenantConfiguration items -------------------------------------------
# Seed per-tenant config into the TenantConfigTable.
# Idempotent: uses --condition-expression so a second run silently skips.
#
# tenant_test: activeEventTypes is non-empty (["order.placed","user.login"])
#              so VAL-003 will REJECT any other type for this tenant.
# tenant_dev:  activeEventTypes is empty -- all registered types are permitted.

$configTable = "streamcore-tenant-config-dev"
$now = (Get-Date -Format "yyyy-MM-ddTHH:mm:ssZ")

$tenantSeedConfigs = @(
    @{
        tenantId         = "tenant_test"
        displayName      = "Test Tenant"
        reportRecipients = @("ops@streamcore.io")
        activeEventTypes = @("order.placed", "user.login")
        status           = "active"
    },
    @{
        tenantId         = "tenant_dev"
        displayName      = "Dev Tenant"
        reportRecipients = @()
        activeEventTypes = @()
        status           = "active"
    }
)

foreach ($cfg in $tenantSeedConfigs) {
    $tid = $cfg.tenantId

    # Look up the ARN of the pii-salt secret we created above.
    $saltArn = awslocal secretsmanager describe-secret `
        --secret-id "streamcore/pii-salt/$tid" `
        --query "ARN" --output text 2>$null
    if (-not $saltArn) { $saltArn = "" }

    # Build the DynamoDB JSON item.
    # reportRecipients and activeEventTypes are L (list) typed.
    $recipL = ($cfg.reportRecipients | ForEach-Object { "{`"S`": `"$_`"}" }) -join ","
    $typesL  = ($cfg.activeEventTypes  | ForEach-Object { "{`"S`": `"$_`"}" }) -join ","

    $itemJson = @"
{
    "tenantId":         {"S": "$tid"},
    "displayName":      {"S": "$($cfg.displayName)"},
    "reportRecipients": {"L": [$recipL]},
    "activeEventTypes": {"L": [$typesL]},
    "status":           {"S": "$($cfg.status)"},
    "piiSaltSecretArn": {"S": "$saltArn"},
    "createdAt":        {"S": "$now"},
    "updatedAt":        {"S": "$now"}
}
"@
    $tmpFile = "$env:TEMP\seed-tcfg-$tid.json"
    $itemJson | Set-Content $tmpFile -Encoding ASCII

    try {
        awslocal dynamodb put-item `
            --table-name $configTable `
            --item "file://$tmpFile" `
            --condition-expression "attribute_not_exists(tenantId)" | Out-Null
        Write-Host "CREATED TenantConfiguration/$tid"
    } catch {
        Write-Host "EXISTS  TenantConfiguration/$tid"
    }

    Remove-Item $tmpFile
}

Write-Host ""
awslocal dynamodb scan `
    --table-name $configTable `
    --query "Items[].{tenant:tenantId.S, types:activeEventTypes.L}" `
    --output table

# ── SES sender identity ──────────────────────────────────────────────────────
Write-Host "`nVerifying SES sender identity..."
awslocal ses verify-email-identity --email-address reports@streamcore.io
Write-Host "VERIFIED  reports@streamcore.io"


Write-Host "`nSeed complete. Current secrets:"
awslocal secretsmanager list-secrets `
    --query "SecretList[].Name" --output table
