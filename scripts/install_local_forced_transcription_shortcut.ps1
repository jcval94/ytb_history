param(
    [string]$RepoDir = (Resolve-Path (Join-Path $PSScriptRoot "..")).Path,
    [string]$PythonPath = "",
    [int]$Limit = 50,
    [int]$ForcedWindowDays = 360,
    [int]$ForcedRefreshMaxPagesPerChannel = 20,
    [string]$ShortcutName = "YTB History - Forced Transcription 360d"
)

$ErrorActionPreference = "Stop"

function Write-JsonNoBom {
    param(
        [string]$Path,
        [object]$Payload
    )

    $utf8NoBom = New-Object System.Text.UTF8Encoding $false
    [System.IO.File]::WriteAllText($Path, (($Payload | ConvertTo-Json -Depth 20) + [Environment]::NewLine), $utf8NoBom)
}

$repoRoot = (Resolve-Path $RepoDir).Path
$playRunnerPath = Join-Path $repoRoot "scripts\run_local_play.ps1"
if (-not (Test-Path $playRunnerPath)) {
    throw "Manual play runner script not found: $playRunnerPath"
}

if (-not $PythonPath) {
    $pythonCommand = Get-Command python -ErrorAction Stop
    $PythonPath = $pythonCommand.Source
}

$desktopPath = [Environment]::GetFolderPath("Desktop")
if (-not $desktopPath) {
    throw "Could not resolve Desktop path."
}

$powershellPath = Join-Path $env:SystemRoot "System32\WindowsPowerShell\v1.0\powershell.exe"
if (-not (Test-Path $powershellPath)) {
    $powershellPath = "powershell.exe"
}

$shortcutPath = Join-Path $desktopPath "$ShortcutName.lnk"
$arguments = '-NoProfile -ExecutionPolicy Bypass -File "{0}" -RepoDir "{1}" -PythonPath "{2}" -Limit {3} -ForcedOnly -ForcedWindowDays {4} -ForcedMaxPerRun {3} -RefreshForcedChannels -ForcedRefreshWindowDays {4} -ForcedRefreshMaxPagesPerChannel {5} -PauseOnExit' -f $playRunnerPath, $repoRoot, $PythonPath, $Limit, $ForcedWindowDays, $ForcedRefreshMaxPagesPerChannel

$shell = New-Object -ComObject WScript.Shell
$shortcut = $shell.CreateShortcut($shortcutPath)
$shortcut.TargetPath = $powershellPath
$shortcut.Arguments = $arguments
$shortcut.WorkingDirectory = $repoRoot
$shortcut.WindowStyle = 1
$shortcut.Description = "Run local sync and transcribe missing videos from forced channels uploaded in the last 360 days."
$shortcut.IconLocation = "$powershellPath,0"
$shortcut.Save()

$automationDir = Join-Path $repoRoot "build\local_automation"
New-Item -ItemType Directory -Force -Path $automationDir | Out-Null
$reportPath = Join-Path $automationDir "latest_forced_transcription_shortcut.json"
$report = [ordered]@{
    generated_at = (Get-Date).ToUniversalTime().ToString("o")
    status = "success"
    shortcut_path = $shortcutPath
    target_path = $powershellPath
    arguments = $arguments
    working_directory = $repoRoot
    python_path = $PythonPath
    limit = $Limit
    forced_window_days = $ForcedWindowDays
    forced_refresh_max_pages_per_channel = $ForcedRefreshMaxPagesPerChannel
}
Write-JsonNoBom -Path $reportPath -Payload $report

Write-Output "Desktop forced transcription shortcut created: $shortcutPath"
Write-Output "It runs: $powershellPath $arguments"
Write-Output "Shortcut report: $reportPath"
