# scripts/seed-events.ps1 — push the event fixtures through the pipeline.
#
# Reads every JSON file in events/valid/ (and optionally events/invalid/),
# stamps it with a fresh tenantId / timestamps / ingestionId, and starts one
# Step Functions execution per event. This is the manual "give me some data"
# helper — the e2e suite seeds its own events.
#
# Usage:
#   ./scripts/seed-events.ps1                 # valid events, tenant_dev (all types allowed)
#   ./scripts/seed-events.ps1 -Tenant tenant_test   # only order.placed / user.login pass VAL-003
#   ./scripts/seed-events.ps1 -IncludeInvalid       # also send the invalid fixtures (go to ValidationDLQ)
#
# Prerequisite: . .\dev-up.ps1  (deploys the stack and seeds configs)

param(
    [string]$Tenant        = "tenant_dev",
    [switch]$IncludeInvalid
)

$ErrorActionPreference = "Stop"

$smArn = awslocal cloudformation describe-stacks `
    --stack-name streamcore-local `
    --query "Stacks[0].Outputs[?OutputKey=='ProcessingStateMachineArn'].OutputValue" `
    --output text
if (-not $smArn) { Write-Error "ProcessingStateMachineArn not found — is the stack deployed?"; exit 1 }

$eventsRoot = Join-Path $PSScriptRoot "..\events"
$dirs = @(Join-Path $eventsRoot "valid")
if ($IncludeInvalid) { $dirs += (Join-Path $eventsRoot "invalid") }

$now = [System.DateTime]::UtcNow.ToString("yyyy-MM-ddTHH:mm:ss.fff") + "Z"
$arns = @()

foreach ($dir in $dirs) {
    foreach ($file in Get-ChildItem $dir -Filter *.json) {
        $obj = Get-Content $file.FullName -Raw | ConvertFrom-Json

        # Stamp the pipeline-side fields the real Ingest Lambda would add.
        # clientTimestamp -> now so the Validator's freshness window passes.
        $ingestionId = [guid]::NewGuid().ToString()
        $obj | Add-Member tenantId        $Tenant      -Force
        $obj | Add-Member clientTimestamp $now         -Force
        $obj | Add-Member ingestedAt      $now         -Force
        $obj | Add-Member ingestionId     $ingestionId -Force

        $tmp = Join-Path $env:TEMP ("seed-evt-" + $file.BaseName + ".json")
        ($obj | ConvertTo-Json -Depth 10 -Compress) | Set-Content $tmp -Encoding ASCII
        $exec = awslocal stepfunctions start-execution `
            --state-machine-arn $smArn --input "file://$tmp" | ConvertFrom-Json
        Remove-Item $tmp
        Write-Host ("started  {0,-22} ({1})" -f $file.Name, $file.Directory.Name)
        $arns += $exec.executionArn
    }
}

Write-Host "`nWaiting for executions to settle..."
Start-Sleep -Seconds 8

$ok = 0; $other = 0
foreach ($arn in $arns) {
    $st = awslocal stepfunctions describe-execution --execution-arn $arn --query "status" --output text
    if ($st -eq "SUCCEEDED") { $ok++ } else { $other++ }
}
Write-Host "Executions: $ok SUCCEEDED, $other other"
Write-Host "(Invalid fixtures also report SUCCEEDED — they are caught and routed to the ValidationDLQ, not failed.)"
Write-Host "`nEvents now in DynamoDB:"
awslocal dynamodb scan --table-name streamcore-events-dev --query "Count"
