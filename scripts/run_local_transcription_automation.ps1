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
    [switch]$ForcedOnly,
    [int]$ForcedWindowDays = 14,
    [int]$ForcedMaxPerRun = 50,
    [switch]$RefreshForcedChannels,
    [int]$ForcedRefreshWindowDays = 360,
    [int]$ForcedRefreshMaxPagesPerChannel = 20,
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

function Write-StatusLine {
    param(
        [int]$Percent,
        [string]$Message
    )

    Write-Output ("[{0,3}%] {1}" -f $Percent, $Message)
}

function Get-IntValue {
    param(
        [object]$Object,
        [string]$Name
    )

    if (-not $Object) {
        return 0
    }
    try {
        return [int]$Object.$Name
    }
    catch {
        return 0
    }
}

function Write-RunSummary {
    param([object]$RunReport)

    if (-not $RunReport) {
        Write-StatusLine -Percent 100 -Message "No se pudo leer latest_run_report.json para resumir la corrida."
        return
    }

    $status = [string]$RunReport.status
    if ($status -and $status -ne "success") {
        Write-StatusLine -Percent 96 -Message "Estado tecnico de la corrida: $status."
        if ($RunReport.steps.youtube_refresh -and $RunReport.steps.youtube_refresh.error) {
            Write-StatusLine -Percent 96 -Message "Refresh de YouTube: $($RunReport.steps.youtube_refresh.error)"
        }
        return
    }

    $selection = $RunReport.steps.transcription_candidates
    $transcription = $RunReport.steps.transcription
    $timestamps = $RunReport.steps.transcript_timestamps
    $insights = $RunReport.steps.transcript_insights
    $git = $RunReport.git
    if ($selection) {
        $selected = Get-IntValue -Object $selection -Name "selected_count"
        $ranked = Get-IntValue -Object $selection -Name "selected_ranked_count"
        $forced = Get-IntValue -Object $selection -Name "selected_forced_count"
        $considered = Get-IntValue -Object $selection -Name "candidates_considered"
        $shortfall = Get-IntValue -Object $selection -Name "ranked_shortfall"
        Write-StatusLine -Percent 84 -Message "Seleccion: $selected videos en cola ($ranked ranking + $forced forzados), $considered candidatos revisados."
        if ($shortfall -gt 0) {
            Write-StatusLine -Percent 84 -Message "Aviso: faltaron $shortfall videos para completar los 10 del ranking; revisar cooldown, ya transcritos y artefactos fuente."
        }
    }
    if ($transcription) {
        $queueTotal = Get-IntValue -Object $transcription -Name "queue_total"
        $processed = Get-IntValue -Object $transcription -Name "processed"
        $success = Get-IntValue -Object $transcription -Name "transcribed_success"
        $downloaded = Get-IntValue -Object $transcription -Name "ytdlp_download_success"
        $failedDownloads = Get-IntValue -Object $transcription -Name "failed_audio_download"
        $failed = Get-IntValue -Object $transcription -Name "failed"
        Write-StatusLine -Percent 88 -Message "Transcripcion: cola $queueTotal, procesados $processed, exitos $success, descargados con yt-dlp $downloaded, fallos audio $failedDownloads, fallos OpenAI $failed."
    }
    if ($timestamps) {
        $processed = Get-IntValue -Object $timestamps -Name "processed"
        $generated = Get-IntValue -Object $timestamps -Name "generated"
        $skippedExisting = Get-IntValue -Object $timestamps -Name "skipped_existing_segments"
        $missingAudio = Get-IntValue -Object $timestamps -Name "skipped_missing_audio"
        $failed = Get-IntValue -Object $timestamps -Name "failed"
        Write-StatusLine -Percent 90 -Message "Timestamps: procesados $processed, generados $generated, ya existentes $skippedExisting, sin audio $missingAudio, fallos $failed."
    }
    if ($insights) {
        $generated = Get-IntValue -Object $insights -Name "generated"
        $cached = Get-IntValue -Object $insights -Name "cached"
        Write-StatusLine -Percent 92 -Message "Insights: $generated nuevos, $cached ya existentes."
    }
    if ($git) {
        if ($git.has_changes -eq $true) {
            Write-StatusLine -Percent 98 -Message "GitHub: hubo cambios publicables en data/transcripts; se intento commit/push."
        }
        else {
            Write-StatusLine -Percent 98 -Message "GitHub: no hubo cambios nuevos que publicar."
        }
    }
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
        forced_only = [bool]$ForcedOnly
        forced_window_days = $ForcedWindowDays
    }
    Write-JsonNoBom -Path $statePath -Payload $newState
    Write-StatusLine -Percent 100 -Message "Transcripcion omitida fuera del horario permitido ($($EarliestHour):00-$($LatestHour):00)."
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
        forced_only = [bool]$ForcedOnly
        forced_window_days = $ForcedWindowDays
    }
    Write-JsonNoBom -Path $statePath -Payload $newState
    Write-StatusLine -Percent 100 -Message "Transcripcion omitida: ya hubo una corrida exitosa recientemente."
    exit 0
}

if (Test-Path $lockPath) {
    $lockAge = $now - (Get-Item $lockPath).LastWriteTime
    if ($lockAge.TotalHours -lt 8) {
        Write-StatusLine -Percent 100 -Message "Transcripcion omitida: ya hay otra corrida en curso. Lock: $lockPath"
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
        "--video-source-dir", "data/video_sources",
        "--progress-log",
        "--no-json-output"
    )
    if ($ForcedOnly) {
        $args += "--forced-only"
    }
    if ($ForcedWindowDays -gt 0) {
        $args += @("--forced-window-days", [string]$ForcedWindowDays)
    }
    if ($ForcedMaxPerRun -gt 0) {
        $args += @("--forced-max-per-run", [string]$ForcedMaxPerRun)
    }
    if ($RefreshForcedChannels) {
        $args += @(
            "--refresh-forced-channels",
            "--forced-refresh-window-days", [string]$ForcedRefreshWindowDays,
            "--forced-refresh-max-pages-per-channel", [string]$ForcedRefreshMaxPagesPerChannel
        )
    }
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
    Write-StatusLine -Percent 0 -Message "Iniciando transcripcion local."
    Write-StatusLine -Percent 5 -Message "Log de transcripcion: $logPath"
    $previousErrorActionPreference = $ErrorActionPreference
    $ErrorActionPreference = "Continue"
    try {
        & $PythonPath @args *>&1 | Tee-Object -FilePath $logPath
        $exitCode = $LASTEXITCODE
    }
    finally {
        $ErrorActionPreference = $previousErrorActionPreference
    }

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
        forced_only = [bool]$ForcedOnly
        forced_window_days = $ForcedWindowDays
    }
    Write-JsonNoBom -Path $statePath -Payload $newState
    Write-RunSummary -RunReport $runReport

    if ($exitCode -ne 0) {
        throw "La automatizacion de transcripcion termino con codigo $exitCode."
    }
    if ($runStatus -ne "success") {
        throw "La automatizacion de transcripcion termino con estado '$runStatus'."
    }
    Write-StatusLine -Percent 100 -Message "Transcripcion local terminada correctamente."
}
finally {
    if (Test-Path $lockPath) {
        Remove-Item -LiteralPath $lockPath -Force
    }
}
