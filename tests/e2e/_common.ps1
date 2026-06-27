# tests/e2e/_common.ps1
#
# Shared helpers, functions, and stack resource lookups for the Phase 6
# per-area e2e suite.  Dot-source this at the TOP of every area file:
#
#   . "$PSScriptRoot\_common.ps1"
#
# This sets $failures = 0 in the CALLING script's scope.  Each area
# file gets its own fresh failure count because each one is a separate
# PowerShell process when launched by run-all.ps1.

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"
$failures = 0

# Absolute path to the event fixture catalog
# (tests/e2e/ -> ../../events = event-ledger/events/)
$FixturesRoot = [System.IO.Path]::GetFullPath((Join-Path $PSScriptRoot "..\..\events"))

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

function Pass { param($label) Write-Host "[PASS] $label" -ForegroundColor Green }
function Fail {
    param($label, $detail)
    Write-Host "[FAIL] $label -- $detail" -ForegroundColor Red
    $script:failures++
}

# Invoke-Post: POST to a URI and return the HTTP status code as an int.
# Works on PowerShell 5.1 (no -SkipHttpErrorCheck); catches WebException to
# extract non-2xx status codes instead of throwing.
function Invoke-Post([string]$Uri, [hashtable]$ExtraHeaders, [string]$Body) {
    $headers = @{ "Content-Type" = "application/json" }
    foreach ($k in $ExtraHeaders.Keys) { $headers[$k] = $ExtraHeaders[$k] }
    try {
        $r = Invoke-WebRequest -Method Post -Uri $Uri -Headers $headers -Body $Body -UseBasicParsing
        return [int]$r.StatusCode
    } catch [System.Net.WebException] {
        return [int]$_.Exception.Response.StatusCode
    }
}

# Invoke-Lambda: invoke a Lambda by function name with a JSON payload string.
# Writes the payload to a temp file (AWS CLI v1 requires file:// for
# binary-safe payloads), reads the response file, and returns the PSObject.
function Invoke-Lambda([string]$FunctionName, [string]$Payload) {
    $tmpIn  = Join-Path $env:TEMP "sc_lmb_in_$([System.IO.Path]::GetRandomFileName()).json"
    $tmpOut = Join-Path $env:TEMP "sc_lmb_out_$([System.IO.Path]::GetRandomFileName()).json"
    Set-Content -Path $tmpIn -Value $Payload -Encoding ASCII
    awslocal lambda invoke `
        --function-name $FunctionName `
        --payload "file://$tmpIn" `
        --output json `
        $tmpOut | Out-Null
    $raw = Get-Content $tmpOut -Raw
    Remove-Item $tmpIn, $tmpOut -ErrorAction SilentlyContinue
    return ($raw | ConvertFrom-Json)
}

# New-LocalJWT: mint a test HS256 JWT for local use.
# The secret must match streamcore/jwt-secret in Secrets Manager (seeded by
# dev-up.ps1).  Uses JWT_SECRET_LOCAL from the session environment.
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

# ---------------------------------------------------------------------------
# Stack resource lookup (runs once per area-file invocation)
# ---------------------------------------------------------------------------

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

$authorizerFn = (awslocal cloudformation describe-stack-resource `
    --stack-name streamcore-local `
    --logical-resource-id AuthorizerFunction `
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

$summaryFn = (awslocal cloudformation describe-stack-resource `
    --stack-name streamcore-local `
    --logical-resource-id SummaryFunction `
    --output json | ConvertFrom-Json).StackResourceDetail.PhysicalResourceId

$validToken = New-LocalJWT -TenantId "tenant_test"

Write-Host "Consumer:       $consumerFn"
Write-Host "Writer:         $writerFn"
Write-Host "Ingest:         $ingestFn"
Write-Host "Authorizer:     $authorizerFn"
Write-Host "State machine:  $smArn"
Write-Host "validToken:     minted for tenant_test"
Write-Host "ValidationDLQ:  $validationDLQUrl"
Write-Host "Summary:        $summaryFn"
Write-Host "Fixtures root:  $FixturesRoot"
