param(
    [string]$SyncTaskName = "YtbHistoryLocalRepoSync",
    [string]$TranscriptionTaskName = "YtbHistoryLocalTranscription",
    [string]$RepoDir = (Resolve-Path (Join-Path $PSScriptRoot "..")).Path,
    [string]$PythonPath = "",
    [int]$Limit = 10,
    [string]$SyncStartAt = "09:00",
    [string]$TranscriptionStartAt = "09:20",
    [string]$WindowEndAt = "23:00",
    [string]$YtdlpCookiesFile = "",
    [string]$YtdlpBrowser = "",
    [string]$YtdlpExtraArgs = "",
    [string]$YtdlpCookiesB64 = ""
)

$ErrorActionPreference = "Stop"

$repoRoot = (Resolve-Path $RepoDir).Path
$syncRunnerPath = Join-Path $repoRoot "scripts\run_local_repo_sync.ps1"
$transcriptionRunnerPath = Join-Path $repoRoot "scripts\run_local_transcription_automation.ps1"
if (-not (Test-Path $syncRunnerPath)) {
    throw "Sync runner script not found: $syncRunnerPath"
}
if (-not (Test-Path $transcriptionRunnerPath)) {
    throw "Transcription runner script not found: $transcriptionRunnerPath"
}

if (-not $PythonPath) {
    $pythonCommand = Get-Command python -ErrorAction Stop
    $PythonPath = $pythonCommand.Source
}

$automationDir = Join-Path $repoRoot "build\local_automation"
New-Item -ItemType Directory -Force -Path $automationDir | Out-Null
$bootstrapLogPath = Join-Path $automationDir "launcher_bootstrap.log"

$syncLauncherPath = Join-Path $automationDir "run_scheduled_repo_sync.cmd"
$syncCatchupLauncherPath = Join-Path $automationDir "run_logon_repo_sync_catchup.cmd"
$transcriptionLauncherPath = Join-Path $automationDir "run_scheduled_transcription.cmd"
$transcriptionCatchupLauncherPath = Join-Path $automationDir "run_logon_transcription_catchup.cmd"

$transcriptionExtraArgs = @()
if ($YtdlpCookiesFile) {
    $transcriptionExtraArgs += "-YtdlpCookiesFile `"$YtdlpCookiesFile`""
}
if ($YtdlpBrowser) {
    $transcriptionExtraArgs += "-YtdlpBrowser `"$YtdlpBrowser`""
}
if ($YtdlpExtraArgs) {
    $transcriptionExtraArgs += "-YtdlpExtraArgs `"$YtdlpExtraArgs`""
}
if ($YtdlpCookiesB64) {
    $transcriptionExtraArgs += "-YtdlpCookiesB64 `"$YtdlpCookiesB64`""
}
$transcriptionExtraArgString = ""
if ($transcriptionExtraArgs.Count -gt 0) {
    $transcriptionExtraArgString = " " + ($transcriptionExtraArgs -join " ")
}

$syncLauncherContent = @"
@echo off
cd /d "$repoRoot"
echo [%date% %time%] Launching scheduled repo sync >> "$bootstrapLogPath"
powershell.exe -NoProfile -ExecutionPolicy Bypass -File "$syncRunnerPath" -RepoDir "$repoRoot" -PythonPath "$PythonPath" >> "$bootstrapLogPath" 2>&1
"@
Set-Content -Path $syncLauncherPath -Value $syncLauncherContent -Encoding ASCII

$syncCatchupContent = @"
@echo off
cd /d "$repoRoot"
echo [%date% %time%] Launching logon repo sync catch-up >> "$bootstrapLogPath"
powershell.exe -NoProfile -ExecutionPolicy Bypass -File "$syncRunnerPath" -RepoDir "$repoRoot" -PythonPath "$PythonPath" -CatchUpOnly >> "$bootstrapLogPath" 2>&1
"@
Set-Content -Path $syncCatchupLauncherPath -Value $syncCatchupContent -Encoding ASCII

$transcriptionLauncherContent = @"
@echo off
cd /d "$repoRoot"
echo [%date% %time%] Launching scheduled transcription >> "$bootstrapLogPath"
powershell.exe -NoProfile -ExecutionPolicy Bypass -File "$transcriptionRunnerPath" -RepoDir "$repoRoot" -PythonPath "$PythonPath" -Limit $Limit$transcriptionExtraArgString >> "$bootstrapLogPath" 2>&1
"@
Set-Content -Path $transcriptionLauncherPath -Value $transcriptionLauncherContent -Encoding ASCII

$transcriptionCatchupContent = @"
@echo off
cd /d "$repoRoot"
echo [%date% %time%] Launching logon transcription catch-up >> "$bootstrapLogPath"
powershell.exe -NoProfile -ExecutionPolicy Bypass -File "$transcriptionRunnerPath" -RepoDir "$repoRoot" -PythonPath "$PythonPath" -Limit $Limit -CatchUpOnly$transcriptionExtraArgString >> "$bootstrapLogPath" 2>&1
"@
Set-Content -Path $transcriptionCatchupLauncherPath -Value $transcriptionCatchupContent -Encoding ASCII

$syncResult = & schtasks /Create /TN $SyncTaskName /SC HOURLY /MO 6 /ST $SyncStartAt /ET $WindowEndAt /TR "`"$syncLauncherPath`"" /F 2>&1
if ($LASTEXITCODE -ne 0) {
    throw ($syncResult | Out-String)
}

$syncCatchupResult = & schtasks /Create /TN "$($SyncTaskName)LogonCatchup" /SC ONLOGON /TR "`"$syncCatchupLauncherPath`"" /F 2>&1
if ($LASTEXITCODE -ne 0) {
    throw ($syncCatchupResult | Out-String)
}

$transcriptionResult = & schtasks /Create /TN $TranscriptionTaskName /SC HOURLY /MO 6 /ST $TranscriptionStartAt /ET $WindowEndAt /TR "`"$transcriptionLauncherPath`"" /F 2>&1
if ($LASTEXITCODE -ne 0) {
    throw ($transcriptionResult | Out-String)
}

$transcriptionCatchupResult = & schtasks /Create /TN "$($TranscriptionTaskName)LogonCatchup" /SC ONLOGON /TR "`"$transcriptionCatchupLauncherPath`"" /F 2>&1
if ($LASTEXITCODE -ne 0) {
    throw ($transcriptionCatchupResult | Out-String)
}

Write-Output "Repo sync scheduled task registered: $SyncTaskName at $SyncStartAt, every 6 hours until $WindowEndAt"
Write-Output "Repo sync logon catch-up registered: $($SyncTaskName)LogonCatchup"
Write-Output "Transcription scheduled task registered: $TranscriptionTaskName at $TranscriptionStartAt, every 6 hours until $WindowEndAt"
Write-Output "Transcription logon catch-up registered: $($TranscriptionTaskName)LogonCatchup"
Write-Output "Generated launchers:"
Write-Output "  $syncLauncherPath"
Write-Output "  $syncCatchupLauncherPath"
Write-Output "  $transcriptionLauncherPath"
Write-Output "  $transcriptionCatchupLauncherPath"
& schtasks /Query /TN $SyncTaskName /V /FO LIST
& schtasks /Query /TN $TranscriptionTaskName /V /FO LIST
