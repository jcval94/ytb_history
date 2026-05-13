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
    Write-Output "Repo sync skipped outside allowed hours: $($now.ToString("o"))"
    exit 0
}

if (Test-Path $lockPath) {
    $lockAge = $now - (Get-Item $lockPath).LastWriteTime
    if ($lockAge.TotalHours -lt 2) {
        Write-Output "Another repo sync run appears active. Lock: $lockPath"
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
        "--repo-dir", $repoRoot
    )
    if ($CatchUpOnly) {
        $args += @("--min-success-interval-hours", [string]$MinSuccessIntervalHours)
    }

    Set-Location $repoRoot
    Write-Output "Running local repo sync. Log: $logPath"
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

    if ($exitCode -ne 0) {
        throw "Local repo sync exited with code $exitCode."
    }
    if ($syncStatus -notin @("success", "up_to_date", "skipped_recent_success")) {
        throw "Local repo sync report status was '$syncStatus'."
    }
}
finally {
    if (Test-Path $lockPath) {
        Remove-Item -LiteralPath $lockPath -Force
    }
}
