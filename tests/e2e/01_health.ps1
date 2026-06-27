# tests/e2e/01_health.ps1  --  Area: Health endpoint  (section 1)
. "$PSScriptRoot\_common.ps1"

Write-Host "`n=== 1. /health ==="
$health = Invoke-RestMethod -Method Get -Uri "$env:LOCAL_BASE_URL/health"

if ($health.status -eq "healthy")     { Pass "status = healthy" }
else                                   { Fail "status" "expected 'healthy', got '$($health.status)'" }

if ($health.checks.dynamodb -eq "ok") { Pass "dynamodb check = ok" }
else                                   { Fail "dynamodb check" "expected 'ok', got '$($health.checks.dynamodb)'" }

Write-Host ""
if ($failures -eq 0) { Write-Host "=== ALL TESTS PASSED ===" -ForegroundColor Green; exit 0 }
else                  { Write-Host "=== $failures TEST(S) FAILED ===" -ForegroundColor Red; exit $failures }
