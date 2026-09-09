# Launch a long capture that survives the machine idling.
#
#   powershell -File scripts\run_capture.ps1              2 hours, BTCUSDT
#   powershell -File scripts\run_capture.ps1 -Hours 8     overnight
#
# An earlier overnight run died at 1h45m because the machine slept. Windows
# suspends the process, close() never runs, and Parquet files with no footer
# are unreadable. File rolling limits what that costs; this stops it happening.

param(
    [double] $Hours = 2,
    [string] $Symbol = "BTCUSDT",
    [string] $Root = "C:\tickforge-runs",
    [double] $HeartbeatSeconds = 300
)

$ErrorActionPreference = "Stop"
New-Item -ItemType Directory -Force -Path $Root | Out-Null

$stamp = (Get-Date).ToUniversalTime().ToString("yyyyMMdd-HHmmss")
$log = Join-Path $Root "capture-$stamp.log"
$err = Join-Path $Root "capture-$stamp.err"

# The flag a media player holds while something is playing. ES_CONTINUOUS keeps
# it set until cleared, ES_SYSTEM_REQUIRED stops sleep, ES_AWAYMODE_REQUIRED
# keeps work running when the machine looks asleep. Released on exit, so no
# permanent power-setting change.
#
# Constants are precomputed decimals, not hex with -bor: Windows PowerShell 5.1
# reads 0x80000000 as a signed Int32, so the combined value comes out negative
# and will not cast to the uint32 the API takes.
$KEEP_AWAKE = [uint32] 2147483713  # ES_CONTINUOUS | ES_SYSTEM_REQUIRED | ES_AWAYMODE_REQUIRED
$RELEASE = [uint32] 2147483648  # ES_CONTINUOUS alone clears the request

Add-Type -Name Power -Namespace Win32 -MemberDefinition @'
[DllImport("kernel32.dll", SetLastError = true)]
public static extern uint SetThreadExecutionState(uint esFlags);
'@
$null = [Win32.Power]::SetThreadExecutionState($KEEP_AWAKE)

$repo = Split-Path -Parent $PSScriptRoot
$proc = Start-Process -FilePath "uv" `
    -ArgumentList "run", "python", "-m", "tickforge", "capture",
                  $Symbol, $Hours, $Root, $HeartbeatSeconds `
    -WorkingDirectory $repo `
    -RedirectStandardOutput $log -RedirectStandardError $err `
    -NoNewWindow -PassThru

Write-Host "capture PID $($proc.Id)  |  $Symbol for $Hours h"
Write-Host "log   $log"
Write-Host "sleep is suppressed until this window exits"
Write-Host ""

try {
    Wait-Process -Id $proc.Id
} finally {
    $null = [Win32.Power]::SetThreadExecutionState($RELEASE)
    Write-Host "`nsleep suppression released"
}

Write-Host "`n=== last 15 log lines ==="
Get-Content $log -Tail 15
Write-Host "`nReport:  uv run python scripts/run_report.py $Root"
