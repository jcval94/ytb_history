param(
    [string]$RepoDir = (Resolve-Path (Join-Path $PSScriptRoot "..")).Path,
    [string]$PythonPath = "python",
    [int]$Limit = 10,
    [switch]$AllowStaleRepo,
    [string]$YtdlpCookiesFile = "",
    [string]$YtdlpBrowser = "",
    [string]$YtdlpExtraArgs = "",
    [string]$YtdlpCookiesB64 = "",
    [switch]$PauseOnExit
)

$ErrorActionPreference = "Stop"

function Read-JsonState {
    param([string]$Path)

    if (-not (Test-Path $Path)) {
        return $null
    }

    try {
        return Get-Content $Path -Raw -Encoding UTF8 | ConvertFrom-Json
    }
    catch {
        return $null
    }
}

function Write-JsonNoBom {
    param(
        [string]$Path,
        [object]$Payload
    )

    $utf8NoBom = New-Object System.Text.UTF8Encoding $false
    [System.IO.File]::WriteAllText($Path, (($Payload | ConvertTo-Json -Depth 30) + [Environment]::NewLine), $utf8NoBom)
}

function Write-PlayLog {
    param(
        [string]$Message,
        [string]$Path
    )

    $line = "[{0}] {1}" -f (Get-Date).ToString("yyyy-MM-dd HH:mm:ss"), $Message
    Write-Output $line
    Add-Content -Path $Path -Value $line -Encoding UTF8
}

$repoRoot = (Resolve-Path $RepoDir).Path
$syncRunnerPath = Join-Path $repoRoot "scripts\run_local_repo_sync.ps1"
$transcriptionRunnerPath = Join-Path $repoRoot "scripts\run_local_transcription_automation.ps1"
if (-not (Test-Path $syncRunnerPath)) {
    throw "Sync runner script not found: $syncRunnerPath"
}
if (-not (Test-Path $transcriptionRunnerPath)) {
    throw "Transcription runner script not found: $transcriptionRunnerPath"
}

$automationDir = Join-Path $repoRoot "build\local_automation"
$logDir = Join-Path $automationDir "logs"
New-Item -ItemType Directory -Force -Path $automationDir, $logDir | Out-Null

$timestamp = (Get-Date).ToString("yyyyMMdd_HHmmss")
$logPath = Join-Path $logDir "manual_play_$timestamp.log"
$playReportPath = Join-Path $automationDir "latest_play_report.json"
$syncReportPath = Join-Path $automationDir "latest_sync_report.json"
$runReportPath = Join-Path $automationDir "latest_run_report.json"
$successSyncStatuses = @("success", "up_to_date", "skipped_recent_success")
$scriptExitCode = 0

$report = [ordered]@{
    generated_at = (Get-Date).ToUniversalTime().ToString("o")
    status = "running"
    repo_dir = $repoRoot
    log_path = $logPath
    limit = $Limit
    allow_stale_repo = [bool]$AllowStaleRepo
    steps = [ordered]@{}
    warnings = @()
}

try {
    Set-Location $repoRoot
    Write-PlayLog -Path $logPath -Message "Starting manual local play."
    Write-PlayLog -Path $logPath -Message "Step 1/2: safe repo sync."

    $syncArgs = @(
        "-NoProfile",
        "-ExecutionPolicy", "Bypass",
        "-File", $syncRunnerPath,
        "-RepoDir", $repoRoot,
        "-PythonPath", $PythonPath,
        "-Force"
    )
    & powershell.exe @syncArgs *>&1 | Tee-Object -FilePath $logPath -Append
    $syncExitCode = $LASTEXITCODE
    $syncReport = Read-JsonState -Path $syncReportPath
    $syncStatus = if ($syncReport) { [string]$syncReport.status } else { "missing_report" }
    $report["steps"]["sync"] = [ordered]@{
        exit_code = $syncExitCode
        status = $syncStatus
        report_path = $syncReportPath
        report = $syncReport
    }

    if (($syncExitCode -ne 0) -or ($successSyncStatuses -notcontains $syncStatus)) {
        $scriptExitCode = if ($syncExitCode -ne 0) { $syncExitCode } else { 1 }
        $report["status"] = "sync_failed_or_blocked"
        $report["warnings"] += "sync_status:$syncStatus"
        Write-JsonNoBom -Path $playReportPath -Payload $report
        Write-PlayLog -Path $logPath -Message "Stopped before transcription. Sync status: $syncStatus"
        return
    }

    Write-PlayLog -Path $logPath -Message "Step 2/2: local transcription and publication."
    $transcriptionArgs = @(
        "-NoProfile",
        "-ExecutionPolicy", "Bypass",
        "-File", $transcriptionRunnerPath,
        "-RepoDir", $repoRoot,
        "-PythonPath", $PythonPath,
        "-Limit", [string]$Limit,
        "-Force"
    )
    if ($AllowStaleRepo) {
        $transcriptionArgs += "-AllowStaleRepo"
    }
    if ($YtdlpCookiesFile) {
        $transcriptionArgs += @("-YtdlpCookiesFile", $YtdlpCookiesFile)
    }
    if ($YtdlpBrowser) {
        $transcriptionArgs += @("-YtdlpBrowser", $YtdlpBrowser)
    }
    if ($YtdlpExtraArgs) {
        $transcriptionArgs += @("-YtdlpExtraArgs", $YtdlpExtraArgs)
    }
    if ($YtdlpCookiesB64) {
        $transcriptionArgs += @("-YtdlpCookiesB64", $YtdlpCookiesB64)
    }

    & powershell.exe @transcriptionArgs *>&1 | Tee-Object -FilePath $logPath -Append
    $transcriptionExitCode = $LASTEXITCODE
    $runReport = Read-JsonState -Path $runReportPath
    $runStatus = if ($runReport) { [string]$runReport.status } else { "missing_report" }
    $report["steps"]["transcription"] = [ordered]@{
        exit_code = $transcriptionExitCode
        status = $runStatus
        report_path = $runReportPath
        report = $runReport
    }

    if (($transcriptionExitCode -eq 0) -and ($runStatus -eq "success")) {
        $report["status"] = "success"
        Write-PlayLog -Path $logPath -Message "Manual local play completed successfully."
    }
    else {
        $scriptExitCode = if ($transcriptionExitCode -ne 0) { $transcriptionExitCode } else { 1 }
        $report["status"] = "transcription_failed"
        $report["warnings"] += "transcription_status:$runStatus"
        Write-PlayLog -Path $logPath -Message "Manual local play finished with transcription status: $runStatus"
    }
}
catch {
    $scriptExitCode = 1
    $report["status"] = "failed"
    $report["error"] = $_.Exception.Message
    Write-PlayLog -Path $logPath -Message "Manual local play failed: $($_.Exception.Message)"
}
finally {
    $report["completed_at"] = (Get-Date).ToUniversalTime().ToString("o")
    $report["exit_code"] = $scriptExitCode
    Write-JsonNoBom -Path $playReportPath -Payload $report
    Write-PlayLog -Path $logPath -Message "Latest play report: $playReportPath"
    if ($PauseOnExit) {
        Write-Output ""
        Read-Host "Press Enter to close this window"
    }
    exit $scriptExitCode
}
