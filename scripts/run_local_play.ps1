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

function Write-ProgressLog {
    param(
        [int]$Percent,
        [string]$Message,
        [string]$Path
    )

    Write-PlayLog -Path $Path -Message ("[{0,3}%] {1}" -f $Percent, $Message)
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

function Get-SyncStatusMessage {
    param([string]$Status)

    switch ($Status) {
        "success" { return "Repo actualizado desde GitHub." }
        "up_to_date" { return "Repo ya estaba al dia con GitHub." }
        "skipped_recent_success" { return "Sync omitido por exito reciente." }
        "blocked_dirty_worktree" { return "Sync bloqueado por cambios locales sin commit." }
        "blocked_non_fast_forward" { return "Sync bloqueado por divergencia con GitHub." }
        default { return "Estado de sync: $Status" }
    }
}

function Write-PlaySummary {
    param(
        [object]$RunReport,
        [string]$Path
    )

    if (-not $RunReport) {
        Write-ProgressLog -Percent 95 -Path $Path -Message "No se pudo leer el reporte final de transcripcion."
        return
    }

    $selection = $RunReport.steps.transcription_candidates
    $transcription = $RunReport.steps.transcription
    $insights = $RunReport.steps.transcript_insights

    if ($selection) {
        $selected = Get-IntValue -Object $selection -Name "selected_count"
        $ranked = Get-IntValue -Object $selection -Name "selected_ranked_count"
        $forced = Get-IntValue -Object $selection -Name "selected_forced_count"
        $considered = Get-IntValue -Object $selection -Name "candidates_considered"
        $shortfall = Get-IntValue -Object $selection -Name "ranked_shortfall"
        Write-ProgressLog -Percent 90 -Path $Path -Message "Cola seleccionada: $selected videos ($ranked ranking + $forced forzados), $considered candidatos revisados."
        if ($shortfall -gt 0) {
            Write-ProgressLog -Percent 90 -Path $Path -Message "Atencion: faltaron $shortfall videos para completar los 10 del ranking. Mira transcript_selection_report.json."
        }
    }

    if ($transcription) {
        $queueTotal = Get-IntValue -Object $transcription -Name "queue_total"
        $processed = Get-IntValue -Object $transcription -Name "processed"
        $success = Get-IntValue -Object $transcription -Name "transcribed_success"
        $downloaded = Get-IntValue -Object $transcription -Name "ytdlp_download_success"
        $failedDownloads = Get-IntValue -Object $transcription -Name "failed_audio_download"
        $failed = Get-IntValue -Object $transcription -Name "failed"
        Write-ProgressLog -Percent 94 -Path $Path -Message "Resultado transcripcion: cola $queueTotal, procesados $processed, exitos $success, yt-dlp $downloaded, fallos audio $failedDownloads, fallos OpenAI $failed."
    }

    if ($insights) {
        $generated = Get-IntValue -Object $insights -Name "generated"
        $cached = Get-IntValue -Object $insights -Name "cached"
        Write-ProgressLog -Percent 96 -Path $Path -Message "Insights: $generated nuevos, $cached ya existentes."
    }

    if ($RunReport.git.has_changes -eq $true) {
        Write-ProgressLog -Percent 98 -Path $Path -Message "Publicacion: se detectaron cambios en data/transcripts y se intento subirlos a GitHub."
    }
    else {
        Write-ProgressLog -Percent 98 -Path $Path -Message "Publicacion: no hubo cambios nuevos que subir a GitHub."
    }
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
    Write-ProgressLog -Percent 0 -Path $logPath -Message "Inicio manual: sincronizar repo, transcribir y publicar resultados."
    Write-ProgressLog -Percent 5 -Path $logPath -Message "Log principal: $logPath"
    Write-ProgressLog -Percent 10 -Path $logPath -Message "Paso 1/2: sincronizacion segura del repo."

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
        Write-ProgressLog -Percent 100 -Path $logPath -Message "Proceso detenido antes de transcribir. $(Get-SyncStatusMessage -Status $syncStatus)"
        return
    }
    Write-ProgressLog -Percent 25 -Path $logPath -Message (Get-SyncStatusMessage -Status $syncStatus)

    Write-ProgressLog -Percent 30 -Path $logPath -Message "Paso 2/2: seleccion, transcripcion, insights y publicacion."
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
    }
    else {
        $scriptExitCode = if ($transcriptionExitCode -ne 0) { $transcriptionExitCode } else { 1 }
        $report["status"] = "transcription_failed"
        $report["warnings"] += "transcription_status:$runStatus"
        Write-PlaySummary -RunReport $runReport -Path $logPath
        Write-ProgressLog -Percent 100 -Path $logPath -Message "Proceso terminado con estado de transcripcion: $runStatus"
    }
    if (($transcriptionExitCode -eq 0) -and ($runStatus -eq "success")) {
        Write-PlaySummary -RunReport $runReport -Path $logPath
        Write-ProgressLog -Percent 100 -Path $logPath -Message "Play local terminado correctamente."
    }
}
catch {
    $scriptExitCode = 1
    $report["status"] = "failed"
    $report["error"] = $_.Exception.Message
    Write-ProgressLog -Percent 100 -Path $logPath -Message "Play local fallo: $($_.Exception.Message)"
}
finally {
    $report["completed_at"] = (Get-Date).ToUniversalTime().ToString("o")
    $report["exit_code"] = $scriptExitCode
    Write-JsonNoBom -Path $playReportPath -Payload $report
    Write-ProgressLog -Percent 100 -Path $logPath -Message "Reporte final: $playReportPath"
    if ($PauseOnExit) {
        Write-Output ""
        Read-Host "Presiona Enter para cerrar esta ventana"
    }
    exit $scriptExitCode
}
