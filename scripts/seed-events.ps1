#!/usr/bin/env pwsh
# scripts/seed-events.ps1
#
# Sends a batch of test events through the processing pipeline.
# Run after seed-localstack.ps1 so PII salts and TenantConfig exist.
#
# Produces:
#   - 3 valid processed events in DynamoDB (2 x order.placed, 1 x user.login)
#     for tenant_test — exercises Summary Lambda counting and top-5 logic
#   - 1 valid event for tenant_dev
#   - 1 invalid event (bad currency) — exercises the ValidationDLQ path;
#     will NOT appear in the Summary Lambda output (rejected events are not
#     written to DynamoDB; rejection count is deferred to Phase 6)

$ErrorActionPreference = "Stop"

$smArn = awslocal cloudformation describe-stacks `
    --stack-name streamcore-local `
    --query "Stacks[0].Outputs[?OutputKey=='ProcessingStateMachineArn'].OutputValue" `
    --output text

if (-not $smArn) {
    Write-Error "Could not find ProcessingStateMachineArn. Is the stack deployed?"
    exit 1
}

$ts = [System.DateTime]::UtcNow.ToString("yyyy-MM-ddTHH:mm:ss") + "Z"

function Send-Event($label, $json) {
    $tmp = "$env:TEMP\seed-evt-$label.json"
    $json.Trim() | Set-Content $tmp -Encoding ASCII
    $exec = awslocal stepfunctions start-execution `
        --state-machine-arn $smArn `
        --input "file://$tmp" | ConvertFrom-Json
    Remove-Item $tmp
    return $exec.executionArn
}

Write-Host "`nStarting executions..."

# Valid events

$arns = @()

$arns += Send-Event "1" @"
{
  "eventId":        "cafe0001-0001-4000-8000-000000000001",
  "eventType":      "order.placed",
  "schemaVersion":  "1.0",
  "tenantId":       "tenant_test",
  "sessionId":      "cafe0001-0001-4000-9000-000000000001",
  "clientTimestamp":"$ts",
  "sdkVersion":     "2.4.1",
  "platform":       "web",
  "ingestedAt":     "$ts",
  "ingestionId":    "cafe0001-0001-4000-a000-000000000001",
  "payload": {
    "orderId": "ORD-SEED-001", "userId": "usr_seed_1",
    "amount": 99.99, "currency": "EUR",
    "items": [{"productId":"P1","productName":"Widget","quantity":2,"unitPrice":49.99}],
    "shippingCountry": "DE"
  }
}
"@

$arns += Send-Event "2" @"
{
  "eventId":        "cafe0001-0002-4000-8000-000000000002",
  "eventType":      "order.placed",
  "schemaVersion":  "1.0",
  "tenantId":       "tenant_test",
  "sessionId":      "cafe0001-0002-4000-9000-000000000002",
  "clientTimestamp":"$ts",
  "sdkVersion":     "2.4.1",
  "platform":       "ios",
  "ingestedAt":     "$ts",
  "ingestionId":    "cafe0001-0002-4000-a000-000000000002",
  "payload": {
    "orderId": "ORD-SEED-002", "userId": "usr_seed_2",
    "amount": 149.00, "currency": "GBP",
    "items": [{"productId":"P2","productName":"Gadget","quantity":1,"unitPrice":149.00}],
    "shippingCountry": "GB"
  }
}
"@

$arns += Send-Event "3" @"
{
  "eventId":        "cafe0001-0003-4000-8000-000000000003",
  "eventType":      "user.login",
  "schemaVersion":  "1.0",
  "tenantId":       "tenant_test",
  "sessionId":      "cafe0001-0003-4000-9000-000000000003",
  "clientTimestamp":"$ts",
  "sdkVersion":     "2.4.1",
  "platform":       "web",
  "ingestedAt":     "$ts",
  "ingestionId":    "cafe0001-0003-4000-a000-000000000003",
  "payload": {
    "userId": "usr_seed_3", "loginMethod": "email", "success": true
  }
}
"@

$arns += Send-Event "4" @"
{
  "eventId":        "cafe0002-0001-4000-8000-000000000004",
  "eventType":      "order.placed",
  "schemaVersion":  "1.0",
  "tenantId":       "tenant_dev",
  "sessionId":      "cafe0002-0001-4000-9000-000000000004",
  "clientTimestamp":"$ts",
  "sdkVersion":     "2.4.1",
  "platform":       "server",
  "ingestedAt":     "$ts",
  "ingestionId":    "cafe0002-0001-4000-a000-000000000004",
  "payload": {
    "orderId": "ORD-DEV-001", "userId": "usr_dev_1",
    "amount": 50.00, "currency": "USD",
    "items": [{"productId":"P3","productName":"Thing","quantity":1,"unitPrice":50.00}],
    "shippingCountry": "US"
  }
}
"@

# Invalid event (bad currency → ValidationDLQ, NOT in DynamoDB)

$arns += Send-Event "5-bad" @"
{
  "eventId":        "cafe0001-0099-4000-8000-000000000099",
  "eventType":      "order.placed",
  "schemaVersion":  "1.0",
  "tenantId":       "tenant_test",
  "sessionId":      "cafe0001-0099-4000-9000-000000000099",
  "clientTimestamp":"$ts",
  "sdkVersion":     "2.4.1",
  "platform":       "web",
  "ingestedAt":     "$ts",
  "ingestionId":    "cafe0001-0099-4000-a000-000000000099",
  "payload": {
    "orderId": "ORD-BAD-001", "userId": "usr_bad",
    "amount": 10.00, "currency": "INVALID",
    "items": [{"productId":"P9","productName":"Bad","quantity":1,"unitPrice":10.00}],
    "shippingCountry": "DE"
  }
}
"@

Write-Host "Waiting for executions to complete..."
Start-Sleep -Seconds 8

# Report results

$passed = 0; $failed = 0
foreach ($arn in $arns) {
    $status = awslocal stepfunctions describe-execution `
        --execution-arn $arn --query "status" --output text
    $label = if ($arn -match "seed-evt-(\S+)") { $Matches[1] } else { $arn.Split(":")[-1] }
    Write-Host "  $arn => $status"
    if ($status -eq "SUCCEEDED") { $passed++ } else { $failed++ }
}

Write-Host "`nExecutions: $passed SUCCEEDED, $failed other"
Write-Host "(All 5 executions return SUCCEEDED - the bad-currency event is caught"
Write-Host " by the Catch block and routed to ValidationDLQ, not a pipeline failure)"

# Confirm DynamoDB has the processed events

Write-Host "`nEvents in DynamoDB:"
awslocal dynamodb scan `
    --table-name streamcore-events-dev `
    --query "Count"
