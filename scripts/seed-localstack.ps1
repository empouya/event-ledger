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

Write-Host "`nSeed complete. Current secrets:"
awslocal secretsmanager list-secrets `
    --query "SecretList[].Name" --output table
