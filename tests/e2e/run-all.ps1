# tests/e2e/run-all.ps1
#
# Runs every per-area e2e file in order and aggregates the exit codes.
# Each area file is a separate PowerShell process so failures stay isolated.
#
# Prerequisites: . .\dev-up.ps1 in the event-ledger/ directory.
# Usage:         .\tests\e2e\run-all.ps1

$scripts = @(
    "01_health.ps1",
    "02_pipeline.ps1",
    "03_ingest_resilience.ps1",
    "04_security.ps1",
    "05_reporting.ps1"
)

$totalFailures = 0
foreach ($s in $scripts) {
    Write-Host "`n$('=' * 60)" -ForegroundColor Cyan
    Write-Host "  Running: $s" -ForegroundColor Cyan
    Write-Host "$('=' * 60)" -ForegroundColor Cyan
    & powershell.exe -NoProfile -File "$PSScriptRoot\$s"
    $exit = $LASTEXITCODE
    $totalFailures += $exit
    if ($exit -gt 0) { Write-Host "[$s] $exit FAILURE(S)" -ForegroundColor Red }
    else              { Write-Host "[$s] PASSED" -ForegroundColor Green }
}

Write-Host "`n$('=' * 60)"
if ($totalFailures -eq 0) {
    Write-Host "=== ALL SCRIPTS PASSED ===" -ForegroundColor Green
    exit 0
} else {
    Write-Host "=== $totalFailures TOTAL FAILURE(S) ===" -ForegroundColor Red
    exit 1
}
