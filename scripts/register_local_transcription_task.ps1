param(
    [string]$TaskName = "YtbHistoryLocalTranscription",
    [string]$RepoDir = (Resolve-Path (Join-Path $PSScriptRoot "..")).Path,
    [string]$PythonPath = "",
    [int]$Limit = 10,
    [string]$At = "09:00"
)

$ErrorActionPreference = "Stop"

$repoRoot = (Resolve-Path $RepoDir).Path
$runnerPath = Join-Path $repoRoot "scripts\run_local_transcription_automation.ps1"
if (-not (Test-Path $runnerPath)) {
    throw "Runner script not found: $runnerPath"
}

if (-not $PythonPath) {
    $pythonCommand = Get-Command python -ErrorAction Stop
    $PythonPath = $pythonCommand.Source
}

$automationDir = Join-Path $repoRoot "build\local_automation"
New-Item -ItemType Directory -Force -Path $automationDir | Out-Null
$launcherPath = Join-Path $automationDir "run_scheduled_transcription.cmd"
$launcherContent = @"
@echo off
cd /d "$repoRoot"
powershell.exe -NoProfile -ExecutionPolicy Bypass -File "$runnerPath" -RepoDir "$repoRoot" -PythonPath "$PythonPath" -Limit $Limit
"@
Set-Content -Path $launcherPath -Value $launcherContent -Encoding ASCII

$weeklyResult = & schtasks /Create /TN $TaskName /SC WEEKLY /D MON,THU /ST $At /TR "`"$launcherPath`"" /F 2>&1
if ($LASTEXITCODE -ne 0) {
    throw ($weeklyResult | Out-String)
}

$startupDir = Join-Path $env:APPDATA "Microsoft\Windows\Start Menu\Programs\Startup"
New-Item -ItemType Directory -Force -Path $startupDir | Out-Null
$startupCmdPath = Join-Path $startupDir "$TaskName`Catchup.cmd"
$startupContent = @"
@echo off
call "$launcherPath"
"@
Set-Content -Path $startupCmdPath -Value $startupContent -Encoding ASCII

Write-Output "Weekly scheduled task registered: $TaskName"
Write-Output "Startup catch-up launcher registered: $startupCmdPath"
Write-Output "Generated launcher: $launcherPath"
& schtasks /Query /TN $TaskName /V /FO LIST
