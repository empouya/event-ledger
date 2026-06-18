#!/usr/bin/env pwsh
# scripts/seed-localstack.ps1
#
# Seeds LocalStack Secrets Manager with per-tenant PII salts required by
# PiiExtractorFunction. Run once after every `docker compose up` before
# deploying or running tests.
#
# Idempotent: secrets that already exist are skipped, not overwritten.
# This preserves hashes produced in the current LocalStack session.

$ErrorActionPreference = "Stop"

$tenants = @("tenant_test", "tenant_dev")

foreach ($tenant in $tenants) {
    $secretName = "streamcore/pii-salt/$tenant"

    # Check whether the secret already exists.
    $existing = awslocal secretsmanager list-secrets `
        --filters Key=name,Values=$secretName | ConvertFrom-Json

    if ($existing.SecretList.Count -gt 0) {
        Write-Host "EXISTS  $secretName"
        continue
    }

    # Generate a 32-byte hex salt and wrap it in the expected JSON shape.
    $salt = -join ((1..32) | ForEach-Object { "{0:x2}" -f (Get-Random -Max 256) })
    @{ piiSalt = $salt } | ConvertTo-Json -Compress `
        | Set-Content "$env:TEMP\seed-$tenant.json" -Encoding ASCII

    awslocal secretsmanager create-secret `
        --name $secretName `
        --secret-string "file://$env:TEMP\seed-$tenant.json" | Out-Null

    Remove-Item "$env:TEMP\seed-$tenant.json"
    Write-Host "CREATED $secretName"
}

Write-Host "`nSeed complete. Current secrets:"
awslocal secretsmanager list-secrets `
    --query "SecretList[].Name" --output table
