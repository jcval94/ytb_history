param(
    [string]$RepoDir = (Resolve-Path (Join-Path $PSScriptRoot "..")).Path,
    [int]$Limit = 10,
    [string]$PythonPath = "python",
    [int]$EarliestHour = 9,
    [int]$LatestHour = 23,
    [double]$MinSuccessIntervalHours = 6,
    [switch]$CatchUpOnly,
    [switch]$Force,
    [switch]$NoSyncGit,
    [switch]$AllowStaleRepo,
    [string]$YtdlpCookiesFile = "",
    [string]$YtdlpBrowser = "",
    [string]$YtdlpExtraArgs = "",
    [string]$YtdlpCookiesB64 = ""
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
    [System.IO.File]::WriteAllText($Path, (($Payload | ConvertTo-Json -Depth 20) + [Environment]::NewLine), $utf8NoBom)
}

function Test-WithinAllowedWindow {
    param(
        [datetime]$Now,
        [int]$Earliest,
        [int]$Latest
    )

    return ($Now.Hour -ge $Earliest) -and ($Now.Hour -lt $Latest)
}

function Test-RecentSuccess {
    param(
        [object]$State,
        [datetime]$Now,
        [double]$MinHours
    )

    if (-not $State -or -not $State.last_success_at) {
        return $false
    }
    try {
        $last = [datetime]::Parse([string]$State.last_success_at)
    }
    catch {
        return $false
    }
    return (($Now.ToUniversalTime() - $last.ToUniversalTime()).TotalHours -lt $MinHours)
}

$repoRoot = (Resolve-Path $RepoDir).Path
$automationDir = Join-Path $repoRoot "build\local_automation"
$logDir = Join-Path $automationDir "logs"
$statePath = Join-Path $automationDir "schedule_state.json"
$latestRunReportPath = Join-Path $automationDir "latest_run_report.json"
$lockPath = Join-Path $automationDir "transcription.lock"
New-Item -ItemType Directory -Force -Path $automationDir, $logDir | Out-Null

$now = Get-Date
$nowUtc = $now.ToUniversalTime().ToString("o")
$state = Read-JsonState -Path $statePath
$previousSuccessAt = if ($state) { [string]$state.last_success_at } else { "" }

if ((-not $Force) -and (-not (Test-WithinAllowedWindow -Now $now -Earliest $EarliestHour -Latest $LatestHour))) {
    $skipReport = [ordered]@{
        generated_at = $nowUtc
        status = "skipped_outside_allowed_hours"
        repo_dir = $repoRoot
        allowed_window = "$($EarliestHour):00-$($LatestHour):00"
        local_time = $now.ToString("o")
        warnings = @()
    }
    Write-JsonNoBom -Path $latestRunReportPath -Payload $skipReport
    $newState = [ordered]@{
        updated_at = $nowUtc
        last_success_at = $previousSuccessAt
        last_exit_code = 0
        last_run_status = "skipped_outside_allowed_hours"
        last_log_path = ""
        repo_dir = $repoRoot
        limit = $Limit
        catch_up_only = [bool]$CatchUpOnly
    }
    Write-JsonNoBom -Path $statePath -Payload $newState
    Write-Output "Local transcription skipped outside allowed hours: $($now.ToString("o"))"
    exit 0
}

if ((-not $Force) -and $CatchUpOnly -and (Test-RecentSuccess -State $state -Now $now -MinHours $MinSuccessIntervalHours)) {
    $skipReport = [ordered]@{
        generated_at = $nowUtc
        status = "skipped_recent_success"
        repo_dir = $repoRoot
        last_success_at = $previousSuccessAt
        warnings = @()
    }
    Write-JsonNoBom -Path $latestRunReportPath -Payload $skipReport
    $newState = [ordered]@{
        updated_at = $nowUtc
        last_success_at = $previousSuccessAt
        last_exit_code = 0
        last_run_status = "skipped_recent_success"
        last_log_path = ""
        repo_dir = $repoRoot
        limit = $Limit
        catch_up_only = [bool]$CatchUpOnly
    }
    Write-JsonNoBom -Path $statePath -Payload $newState
    Write-Output "Local transcription skipped because a successful run is recent."
    exit 0
}

if (Test-Path $lockPath) {
    $lockAge = $now - (Get-Item $lockPath).LastWriteTime
    if ($lockAge.TotalHours -lt 8) {
        Write-Output "Another local transcription run appears active. Lock: $lockPath"
        exit 0
    }
    Remove-Item -LiteralPath $lockPath -Force
}

Set-Content -Path $lockPath -Value $now.ToString("o") -Encoding UTF8

try {
    $timestamp = $now.ToString("yyyyMMdd_HHmmss")
    $logPath = Join-Path $logDir "local_transcription_$timestamp.log"
    $args = @(
        "-m", "ytb_history.cli", "run-local-transcription-automation",
        "--repo-dir", $repoRoot,
        "--data-dir", "data",
        "--skip-youtube-refresh",
        "--limit", [string]$Limit,
        "--audio-source-dir", "data/audio_sources",
        "--video-source-dir", "data/video_sources"
    )
    if ($NoSyncGit) {
        $args += "--no-sync-git"
    }
    if ($AllowStaleRepo) {
        $args += "--allow-stale-repo"
    }
    if ($YtdlpCookiesFile) {
        $args += @("--ytdlp-cookies-file", $YtdlpCookiesFile)
    }
    if ($YtdlpBrowser) {
        $args += @("--ytdlp-browser", $YtdlpBrowser)
    }
    if ($YtdlpExtraArgs) {
        $args += @("--ytdlp-extra-args", $YtdlpExtraArgs)
    }
    if ($YtdlpCookiesB64) {
        $args += @("--ytdlp-cookies-b64", $YtdlpCookiesB64)
    }

    Set-Location $repoRoot
    Write-Output "Running local transcription automation. Log: $logPath"
    & $PythonPath @args *>&1 | Tee-Object -FilePath $logPath
    $exitCode = $LASTEXITCODE

    $runReport = Read-JsonState -Path $latestRunReportPath
    $runStatus = if ($runReport) { [string]$runReport.status } else { "unknown" }
    $stateSuccessAt = if (($exitCode -eq 0) -and ($runStatus -eq "success")) { (Get-Date).ToUniversalTime().ToString("o") } else { $previousSuccessAt }

    $newState = [ordered]@{
        updated_at = (Get-Date).ToUniversalTime().ToString("o")
        last_success_at = $stateSuccessAt
        last_exit_code = $exitCode
        last_run_status = $runStatus
        last_log_path = $logPath
        repo_dir = $repoRoot
        limit = $Limit
        catch_up_only = [bool]$CatchUpOnly
    }
    Write-JsonNoBom -Path $statePath -Payload $newState

    if ($exitCode -ne 0) {
        throw "Local transcription automation exited with code $exitCode."
    }
    if ($runStatus -ne "success") {
        throw "Local transcription automation report status was '$runStatus'."
    }
}
finally {
    if (Test-Path $lockPath) {
        Remove-Item -LiteralPath $lockPath -Force
    }
}
