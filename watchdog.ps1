# Watchdog for kalshi_btc/main_runner.py
# Checks every 5 minutes. Restarts the runner if it is not alive.
# Registered in Task Scheduler to start at every user logon.

$scriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$logFile   = Join-Path $scriptDir "dry_run_output.log"
$batFile   = Join-Path $scriptDir "launch_runner.bat"

# Prevent multiple simultaneous watchdog instances
$mutex = New-Object System.Threading.Mutex($false, "KalshiBTCWatchdog")
if (-not $mutex.WaitOne(0)) {
    exit 0
}

function Show-Toast {
    param([string]$Title, [string]$Body)
    try {
        [void][Windows.UI.Notifications.ToastNotificationManager, Windows.UI.Notifications, ContentType=WindowsRuntime]
        [void][Windows.Data.Xml.Dom.XmlDocument, Windows.Data.Xml.Dom.XmlDocument, ContentType=WindowsRuntime]
        $xml = '<toast><visual><binding template="ToastGeneric"><text>' + $Title + '</text><text>' + $Body + '</text></binding></visual></toast>'
        $doc = New-Object Windows.Data.Xml.Dom.XmlDocument
        $doc.LoadXml($xml)
        $toast = [Windows.UI.Notifications.ToastNotification]::new($doc)
        [Windows.UI.Notifications.ToastNotificationManager]::CreateToastNotifier("Microsoft.Windows.Explorer").Show($toast)
    } catch {}
}

function Test-RunnerAlive {
    $procs = Get-CimInstance Win32_Process -Filter "Name='python.exe'" -ErrorAction SilentlyContinue
    return ($null -ne ($procs | Where-Object { $_.CommandLine -like "*main_runner.py*" }))
}

try {
    while ($true) {
        if (-not (Test-RunnerAlive)) {
            $ts = Get-Date -Format "yyyy-MM-ddTHH:mm:ss"
            Add-Content -Path $logFile -Value ""
            Add-Content -Path $logFile -Value "[WATCHDOG $ts] Runner not found -- restarting..."
            Start-Process -FilePath "cmd.exe" -ArgumentList "/c `"$batFile`"" -WindowStyle Hidden
            Show-Toast -Title "Kalshi BTC Runner Restarted" -Body "main_runner.py stopped. Watchdog restarted at $ts"
        }
        Start-Sleep -Seconds 300
    }
} finally {
    $mutex.ReleaseMutex()
}
