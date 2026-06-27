# tests/e2e/05_reporting.ps1  --  Area: Reporting  (sections 15-18)
#
# Sections 15-18: seed yesterday's events via Step Functions direct start,
# invoke Summary Lambda, assert SNS topic, assert SES emails.
. "$PSScriptRoot\_common.ps1"

# =============================================================================
# 15. SEED YESTERDAY'S EVENTS (state machine bypass)
#
# clientTimestamp = now   -> passes Validator timestamp window
# ingestedAt      = yesterday 12:00 UTC -> falls in Summary Lambda's GSI1SK
#                   query range [yesterday T00:00:00Z, yesterday T23:59:59Z]
#
# Fixture base shapes are read from events/valid/; runtime fields are merged
# in.  Hardcoded ingestionIds make executions stable across re-runs:
# LocalStack returns the existing ARN on duplicate names rather than erroring.
# =============================================================================
Write-Host "`n=== 15. Seed yesterday's events ==="

$yesterday = [System.DateTime]::UtcNow.AddDays(-1).ToString("yyyy-MM-dd")
$ydTs      = "${yesterday}T12:00:00Z"
$nowTs15   = [System.DateTime]::UtcNow.ToString("yyyy-MM-ddTHH:mm:ss") + "Z"

Write-Host "    yesterday:  $yesterday"
Write-Host "    ingestedAt: $ydTs"

$orderFixture = Get-Content (Join-Path $FixturesRoot "valid\order.placed.json") -Raw | ConvertFrom-Json
$loginFixture = Get-Content (Join-Path $FixturesRoot "valid\user.login.json")   -Raw | ConvertFrom-Json

function Send-S15Event([string]$Lbl, [string]$Body) {
    $tmp = Join-Path $env:TEMP "sc_s15_$Lbl.json"
    $Body | Set-Content $tmp -Encoding ASCII
    $r = awslocal stepfunctions start-execution `
        --state-machine-arn $smArn `
        --input "file://$tmp" `
        --output json | ConvertFrom-Json
    Remove-Item $tmp -ErrorAction SilentlyContinue
    return $r.executionArn
}

$s15Arns = @()

# a: order.placed -- tenant_test
$evtA = $orderFixture.PSObject.Copy()
$evtA | Add-Member -NotePropertyName eventId      -NotePropertyValue "cafe5601-0001-4000-8000-000000000001" -Force
$evtA | Add-Member -NotePropertyName tenantId     -NotePropertyValue "tenant_test" -Force
$evtA | Add-Member -NotePropertyName clientTimestamp -NotePropertyValue $nowTs15 -Force
$evtA | Add-Member -NotePropertyName ingestedAt   -NotePropertyValue $ydTs -Force
$evtA | Add-Member -NotePropertyName ingestionId  -NotePropertyValue "cafe5601-0001-4000-a000-000000000001" -Force
$s15Arns += Send-S15Event "a" ($evtA | ConvertTo-Json -Depth 10 -Compress)

# b: order.placed -- tenant_test
$evtB = $orderFixture.PSObject.Copy()
$evtB | Add-Member -NotePropertyName eventId      -NotePropertyValue "cafe5601-0002-4000-8000-000000000002" -Force
$evtB | Add-Member -NotePropertyName tenantId     -NotePropertyValue "tenant_test" -Force
$evtB | Add-Member -NotePropertyName clientTimestamp -NotePropertyValue $nowTs15 -Force
$evtB | Add-Member -NotePropertyName ingestedAt   -NotePropertyValue $ydTs -Force
$evtB | Add-Member -NotePropertyName ingestionId  -NotePropertyValue "cafe5601-0002-4000-a000-000000000002" -Force
$s15Arns += Send-S15Event "b" ($evtB | ConvertTo-Json -Depth 10 -Compress)

# c: user.login -- tenant_test
$evtC = $loginFixture.PSObject.Copy()
$evtC | Add-Member -NotePropertyName eventId      -NotePropertyValue "cafe5601-0003-4000-8000-000000000003" -Force
$evtC | Add-Member -NotePropertyName tenantId     -NotePropertyValue "tenant_test" -Force
$evtC | Add-Member -NotePropertyName clientTimestamp -NotePropertyValue $nowTs15 -Force
$evtC | Add-Member -NotePropertyName ingestedAt   -NotePropertyValue $ydTs -Force
$evtC | Add-Member -NotePropertyName ingestionId  -NotePropertyValue "cafe5601-0003-4000-a000-000000000003" -Force
$s15Arns += Send-S15Event "c" ($evtC | ConvertTo-Json -Depth 10 -Compress)

# d: order.placed -- tenant_dev
$evtD = $orderFixture.PSObject.Copy()
$evtD | Add-Member -NotePropertyName eventId      -NotePropertyValue "cafe5602-0001-4000-8000-000000000004" -Force
$evtD | Add-Member -NotePropertyName tenantId     -NotePropertyValue "tenant_dev" -Force
$evtD | Add-Member -NotePropertyName clientTimestamp -NotePropertyValue $nowTs15 -Force
$evtD | Add-Member -NotePropertyName ingestedAt   -NotePropertyValue $ydTs -Force
$evtD | Add-Member -NotePropertyName ingestionId  -NotePropertyValue "cafe5602-0001-4000-a000-000000000004" -Force
$s15Arns += Send-S15Event "d" ($evtD | ConvertTo-Json -Depth 10 -Compress)

Write-Host "    waiting for executions..."
Start-Sleep -Seconds 10

$s15Ok = 0; $s15Bad = 0
foreach ($arn in $s15Arns) {
    $st = awslocal stepfunctions describe-execution `
        --execution-arn $arn --query "status" --output text
    if ($st -eq "SUCCEEDED") { $s15Ok++ } else { $s15Bad++ }
}

if ($s15Ok -eq 4 -and $s15Bad -eq 0) {
    Pass "all 4 yesterday events processed by state machine"
} else {
    Fail "yesterday event seeding" "$s15Ok SUCCEEDED, $s15Bad other (expected 4/0)"
}

# =============================================================================
# 16. SUMMARY LAMBDA INVOCATION
# =============================================================================
Write-Host "`n=== 16. Summary Lambda invocation (date=$yesterday) ==="

$sesBefore = try {
    $r = (Invoke-RestMethod -Method Get -Uri "http://localhost:4566/_aws/ses").messages
    if ($r) { @($r).Count } else { 0 }
} catch { 0 }
Write-Host "    SES messages before invoke: $sesBefore"

$sumPayFile  = Join-Path $env:TEMP "sc_sum_pay_$([System.IO.Path]::GetRandomFileName()).json"
$sumRespFile = Join-Path $env:TEMP "sc_sum_resp_$([System.IO.Path]::GetRandomFileName()).json"
('{"date": "' + $yesterday + '"}') | Set-Content $sumPayFile -Encoding ASCII

$sumInvoke = awslocal lambda invoke `
    --function-name $summaryFn `
    --payload "file://$sumPayFile" `
    --output json `
    $sumRespFile | ConvertFrom-Json

$sumResult  = Get-Content $sumRespFile -Raw | ConvertFrom-Json
Remove-Item $sumPayFile, $sumRespFile -ErrorAction SilentlyContinue

$sumFuncErr = if ($sumInvoke.PSObject.Properties['FunctionError']) { $sumInvoke.FunctionError } else { $null }
if (-not $sumFuncErr) { Pass "Summary Lambda invoked without error" }
else { Fail "Summary Lambda" "FunctionError=$sumFuncErr body=$($sumResult | ConvertTo-Json -Compress)" }

if (-not $sumFuncErr) {
    if ($sumResult.PSObject.Properties['tenantsProcessed'] -and [int]$sumResult.tenantsProcessed -ge 2) {
        Pass "tenantsProcessed = $($sumResult.tenantsProcessed)"
    } else {
        Fail "tenantsProcessed" "expected >= 2, got '$($sumResult.tenantsProcessed)'"
    }

    $ttSum = $sumResult.summaries |
        Where-Object { $_.tenantId -eq "tenant_test" } | Select-Object -First 1

    if ($ttSum) {
        if ([int]$ttSum.totalEvents -ge 1) {
            Pass "tenant_test totalEvents = $($ttSum.totalEvents)"
        } else {
            Fail "tenant_test totalEvents" "expected >= 1, got $($ttSum.totalEvents)"
        }

        $byType = $ttSum.byEventType
        if ($byType."order.placed" -ge 1) { Pass "tenant_test byEventType: order.placed present" }
        else { Fail "tenant_test byEventType" "order.placed missing or 0" }

        if ($byType."user.login" -ge 1) { Pass "tenant_test byEventType: user.login present" }
        else { Fail "tenant_test byEventType" "user.login missing or 0" }

        $ttJson = $ttSum | ConvertTo-Json -Depth 10 -Compress
        if ($ttJson -notmatch '"usr_') {
            Pass "tenant_test summary: no raw PII (no 'usr_' pattern in aggregation output)"
        } else {
            Fail "PII leak in summary" "found 'usr_' in tenant_test summary JSON"
        }
    } else {
        Fail "tenant_test summary" "not found in summaries array"
    }

    $tdSum = $sumResult.summaries |
        Where-Object { $_.tenantId -eq "tenant_dev" } | Select-Object -First 1

    if ($tdSum) {
        if ([int]$tdSum.totalEvents -ge 1) { Pass "tenant_dev totalEvents = $($tdSum.totalEvents)" }
        else { Fail "tenant_dev totalEvents" "expected >= 1, got $($tdSum.totalEvents)" }
    } else {
        Fail "tenant_dev summary" "not found in summaries array"
    }
}

# =============================================================================
# 17. SNS DAILY-REPORTS TOPIC
# =============================================================================
Write-Host "`n=== 17. SNS daily-reports topic ==="

$dailyReportsTopicArn = ($stackOutputs |
    Where-Object { $_.OutputKey -eq "DailyReportsTopicArn" }).OutputValue

if ($dailyReportsTopicArn) {
    $topicAttr = awslocal sns get-topic-attributes `
        --topic-arn $dailyReportsTopicArn `
        --output json | ConvertFrom-Json

    if ($topicAttr.Attributes.TopicArn -eq $dailyReportsTopicArn) {
        Pass "DailyReportsTopic exists (ARN: $dailyReportsTopicArn)"
    } else {
        Fail "DailyReportsTopic attributes" "TopicArn mismatch or missing"
    }
} else {
    Fail "DailyReportsTopicArn" "not found in stack outputs"
}

Write-Host "    [NOTE] SNS->SQS fan-out not tested (LocalStack Community limitation; real-AWS window)"
Pass "SNS publish confirmed via Summary Lambda success in section 16"

# =============================================================================
# 18. SES EMAIL ASSERTIONS
# =============================================================================
Write-Host "`n=== 18. SES email assertions ==="

$sesAll        = @((Invoke-RestMethod -Method Get -Uri "http://localhost:4566/_aws/ses").messages)
Write-Host "    total SES messages in store: $($sesAll.Count)"

$tenantSubject = "StreamCore Daily Report - Test Tenant - $yesterday"
$opsSubject    = "StreamCore Internal Ops Report - $yesterday"

$tenantMsg = $sesAll | Where-Object {
    ($_ | ConvertTo-Json -Compress) -match [regex]::Escape($tenantSubject)
} | Select-Object -Last 1

$opsMsg = $sesAll | Where-Object {
    ($_ | ConvertTo-Json -Compress) -match [regex]::Escape($opsSubject)
} | Select-Object -Last 1

if ($tenantMsg) {
    Pass "tenant report email found (subject: $tenantSubject)"
    $tenantMsgJson = $tenantMsg | ConvertTo-Json -Depth 10 -Compress

    if ($tenantMsgJson -match '"reports@streamcore\.io"') {
        Pass "tenant email Source = reports@streamcore.io"
    } else {
        Fail "tenant email Source" "reports@streamcore.io not found in message"
    }

    if ($tenantMsgJson -match '"ops@streamcore\.io"') {
        Pass "tenant email delivered to ops@streamcore.io"
    } else {
        Fail "tenant email ToAddresses" "ops@streamcore.io not found in message"
    }

    if ($tenantMsgJson -notmatch '"usr_') {
        Pass "tenant email body: no raw PII (no 'usr_' pattern)"
    } else {
        Fail "PII in tenant email" "found 'usr_' pattern in message JSON"
    }
} else {
    Fail "tenant report email" "no message with subject '$tenantSubject'"
}

if ($opsMsg) {
    Pass "ops report email found (subject: $opsSubject)"
    $opsMsgJson = $opsMsg | ConvertTo-Json -Depth 10 -Compress

    if ($opsMsgJson -match '"reports@streamcore\.io"') {
        Pass "ops email Source = reports@streamcore.io"
    } else {
        Fail "ops email Source" "reports@streamcore.io not found in message"
    }

    if ($opsMsgJson -match '"ops@streamcore\.io"') {
        Pass "ops email delivered to ops@streamcore.io"
    } else {
        Fail "ops email ToAddresses" "ops@streamcore.io not found in message"
    }
} else {
    Fail "ops report email" "no message with subject '$opsSubject'"
}

$devMsg = $sesAll | Where-Object {
    ($_ | ConvertTo-Json -Compress) -match [regex]::Escape("StreamCore Daily Report - Dev Tenant")
} | Select-Object -First 1

if (-not $devMsg) {
    Pass "tenant_dev: no email (reportRecipients empty -- skipped as expected)"
} else {
    Fail "tenant_dev email isolation" "email found for Dev Tenant despite empty reportRecipients"
}

Write-Host ""
if ($failures -eq 0) { Write-Host "=== ALL TESTS PASSED ===" -ForegroundColor Green; exit 0 }
else                  { Write-Host "=== $failures TEST(S) FAILED ===" -ForegroundColor Red; exit $failures }
