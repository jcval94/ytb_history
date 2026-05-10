param(
    [string]$RepoDir = (Resolve-Path (Join-Path $PSScriptRoot "..")).Path,
    [int]$Limit = 10,
    [string]$PythonPath = "python",
    [string[]]$ScheduledDays = @("Monday", "Thursday"),
    [int]$CatchUpWindowDays = 4,
    [switch]$Force,
    [switch]$NoSyncGit
)

$ErrorActionPreference = "Stop"

function Get-LastScheduledDate {
    param(
        [datetime]$Today,
        [string[]]$Days,
        [int]$WindowDays
    )

    $scheduled = @{}
    foreach ($day in $Days) {
        $scheduled[[System.Enum]::Parse([System.DayOfWeek], $day, $true)] = $true
    }

    for ($offset = 0; $offset -le $WindowDays; $offset++) {
        $candidate = $Today.AddDays(-1 * $offset)
        if ($scheduled.ContainsKey($candidate.DayOfWeek)) {
            return $candidate
        }
    }
    return $null
}

function Read-JsonState {
    param([string]$Path)

    if (-not (Test-Path $Path)) {
        return $null
    }

    try {
        return Get-Content $Path -Raw | ConvertFrom-Json
    }
    catch {
        return $null
    }
}

$repoRoot = (Resolve-Path $RepoDir).Path
$automationDir = Join-Path $repoRoot "build\local_automation"
$logDir = Join-Path $automationDir "logs"
$statePath = Join-Path $automationDir "schedule_state.json"
$lockPath = Join-Path $automationDir "transcription.lock"
New-Item -ItemType Directory -Force -Path $automationDir, $logDir | Out-Null

$now = Get-Date
$today = $now.Date
$lastDueDate = Get-LastScheduledDate -Today $today -Days $ScheduledDays -WindowDays $CatchUpWindowDays
$lastDueKey = if ($lastDueDate) { $lastDueDate.ToString("yyyy-MM-dd") } else { $null }
$state = Read-JsonState -Path $statePath
$lastSuccessDueDate = if ($state) { [string]$state.last_success_due_date } else { "" }

if (-not $Force) {
    if (-not $lastDueDate) {
        Write-Output "No scheduled run is due within the catch-up window."
        exit 0
    }
    if ($lastSuccessDueDate -eq $lastDueKey) {
        Write-Output "Local transcription already completed for due date $lastDueKey."
        exit 0
    }
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
        "--limit", [string]$Limit
    )
    if ($NoSyncGit) {
        $args += "--no-sync-git"
    }

    Set-Location $repoRoot
    Write-Output "Running local transcription automation. Log: $logPath"
    & $PythonPath @args *>&1 | Tee-Object -FilePath $logPath
    $exitCode = $LASTEXITCODE

    $runReportPath = Join-Path $automationDir "latest_run_report.json"
    $runReport = Read-JsonState -Path $runReportPath
    $runStatus = if ($runReport) { [string]$runReport.status } else { "unknown" }

    $newState = [ordered]@{
        updated_at = (Get-Date).ToUniversalTime().ToString("o")
        last_due_date = $lastDueKey
        last_success_due_date = if (($exitCode -eq 0) -and ($runStatus -eq "success")) { $lastDueKey } else { $lastSuccessDueDate }
        last_exit_code = $exitCode
        last_run_status = $runStatus
        last_log_path = $logPath
        repo_dir = $repoRoot
        limit = $Limit
    }
    $utf8NoBom = New-Object System.Text.UTF8Encoding $false
    [System.IO.File]::WriteAllText($statePath, (($newState | ConvertTo-Json -Depth 5) + [Environment]::NewLine), $utf8NoBom)

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
