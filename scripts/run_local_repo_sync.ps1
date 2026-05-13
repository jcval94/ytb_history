param(
    [string]$RepoDir = (Resolve-Path (Join-Path $PSScriptRoot "..")).Path,
    [string]$PythonPath = "python",
    [int]$EarliestHour = 9,
    [int]$LatestHour = 23,
    [double]$MinSuccessIntervalHours = 6,
    [switch]$CatchUpOnly,
    [switch]$Force
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

function Get-SyncStatusMessage {
    param([string]$Status)

    switch ($Status) {
        "success" { return "Repo actualizado con git pull --ff-only." }
        "up_to_date" { return "Repo ya estaba al dia con origin/main." }
        "skipped_recent_success" { return "Sync omitido porque hubo uno exitoso recientemente." }
        "blocked_dirty_worktree" { return "Sync bloqueado: hay cambios tracked locales sin commit." }
        "blocked_non_fast_forward" { return "Sync bloqueado: hay commits locales o divergencia con origin/main." }
        "failed" { return "Sync fallo; revisa el reporte tecnico." }
        default { return "Estado de sync: $Status" }
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

$repoRoot = (Resolve-Path $RepoDir).Path
$automationDir = Join-Path $repoRoot "build\local_automation"
$logDir = Join-Path $automationDir "logs"
$statePath = Join-Path $automationDir "sync_state.json"
$latestReportPath = Join-Path $automationDir "latest_sync_report.json"
$lockPath = Join-Path $automationDir "repo_sync.lock"
New-Item -ItemType Directory -Force -Path $automationDir, $logDir | Out-Null

$now = Get-Date
$nowUtc = $now.ToUniversalTime().ToString("o")

if ((-not $Force) -and (-not (Test-WithinAllowedWindow -Now $now -Earliest $EarliestHour -Latest $LatestHour))) {
    $skipReport = [ordered]@{
        generated_at = $nowUtc
        status = "skipped_outside_allowed_hours"
        repo_dir = $repoRoot
        allowed_window = "$($EarliestHour):00-$($LatestHour):00"
        local_time = $now.ToString("o")
        git = @{ action = "skipped_outside_allowed_hours" }
        warnings = @()
    }
    Write-JsonNoBom -Path $latestReportPath -Payload $skipReport
    Write-StatusLine -Percent 100 -Message "Sync omitido fuera del horario permitido ($($EarliestHour):00-$($LatestHour):00)."
    exit 0
}

if (Test-Path $lockPath) {
    $lockAge = $now - (Get-Item $lockPath).LastWriteTime
    if ($lockAge.TotalHours -lt 2) {
        Write-StatusLine -Percent 100 -Message "Sync omitido: ya hay otra sincronizacion en curso. Lock: $lockPath"
        exit 0
    }
    Remove-Item -LiteralPath $lockPath -Force
}

Set-Content -Path $lockPath -Value $now.ToString("o") -Encoding UTF8

try {
    $timestamp = $now.ToString("yyyyMMdd_HHmmss")
    $logPath = Join-Path $logDir "repo_sync_$timestamp.log"
    $args = @(
        "-m", "ytb_history.cli", "sync-local-repo",
        "--repo-dir", $repoRoot,
        "--progress-log",
        "--no-json-output"
    )
    if ($CatchUpOnly) {
        $args += @("--min-success-interval-hours", [string]$MinSuccessIntervalHours)
    }

    Set-Location $repoRoot
    Write-StatusLine -Percent 0 -Message "Iniciando sincronizacion segura del repositorio."
    Write-StatusLine -Percent 5 -Message "Log de sync: $logPath"
    & $PythonPath @args *>&1 | Tee-Object -FilePath $logPath
    $exitCode = $LASTEXITCODE

    $syncReport = Read-JsonState -Path $latestReportPath
    $syncStatus = if ($syncReport) { [string]$syncReport.status } else { "unknown" }
    $successStatuses = @("success", "up_to_date", "skipped_recent_success")
    $lastSuccessAt = ""
    if ($syncReport -and $syncReport.last_success_at) {
        $lastSuccessAt = [string]$syncReport.last_success_at
    }
    $previousState = Read-JsonState -Path $statePath
    $previousSuccessAt = if ($previousState) { [string]$previousState.last_success_at } else { "" }
    $stateSuccessAt = if (($exitCode -eq 0) -and ($successStatuses -contains $syncStatus)) { $lastSuccessAt } else { $previousSuccessAt }

    $newState = [ordered]@{
        updated_at = (Get-Date).ToUniversalTime().ToString("o")
        last_success_at = $stateSuccessAt
        last_exit_code = $exitCode
        last_sync_status = $syncStatus
        last_log_path = $logPath
        repo_dir = $repoRoot
        catch_up_only = [bool]$CatchUpOnly
    }
    Write-JsonNoBom -Path $statePath -Payload $newState
    Write-StatusLine -Percent 100 -Message (Get-SyncStatusMessage -Status $syncStatus)

    if ($exitCode -ne 0) {
        throw "La sincronizacion del repo termino con codigo $exitCode."
    }
    if ($syncStatus -notin @("success", "up_to_date", "skipped_recent_success")) {
        throw "La sincronizacion del repo termino con estado '$syncStatus'."
    }
}
finally {
    if (Test-Path $lockPath) {
        Remove-Item -LiteralPath $lockPath -Force
    }
}
