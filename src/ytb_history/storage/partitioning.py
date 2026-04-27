"""Path partitioning helpers for immutable run outputs."""

from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path


def _as_utc(execution_date: datetime) -> datetime:
    if execution_date.tzinfo is None:
        return execution_date.replace(tzinfo=timezone.utc)
    return execution_date.astimezone(timezone.utc)


def make_run_id(execution_date: datetime) -> str:
    execution_date_utc = _as_utc(execution_date)
    return execution_date_utc.strftime("%H%M%SZ")


def make_dt_partition(execution_date: datetime) -> str:
    execution_date_utc = _as_utc(execution_date)
    return f"dt={execution_date_utc.strftime('%Y-%m-%d')}"


def _run_dir(execution_date: datetime, base_dir: str | Path) -> Path:
    root = Path(base_dir)
    return root / make_dt_partition(execution_date) / f"run={make_run_id(execution_date)}"


def snapshot_path_for_run(execution_date: datetime, base_dir: str | Path = "data/snapshots") -> Path:
    return _run_dir(execution_date, base_dir) / "snapshots.jsonl.gz"


def delta_path_for_run(execution_date: datetime, base_dir: str | Path = "data/deltas") -> Path:
    return _run_dir(execution_date, base_dir) / "deltas.jsonl.gz"


def report_dir_for_run(execution_date: datetime, base_dir: str | Path = "data/reports") -> Path:
    return _run_dir(execution_date, base_dir)


def quota_report_path_for_run(execution_date: datetime, base_dir: str | Path = "data/reports") -> Path:
    return report_dir_for_run(execution_date, base_dir=base_dir) / "quota_report.json"


def run_summary_path_for_run(execution_date: datetime, base_dir: str | Path = "data/reports") -> Path:
    return report_dir_for_run(execution_date, base_dir=base_dir) / "run_summary.json"


def discovery_report_path_for_run(execution_date: datetime, base_dir: str | Path = "data/reports") -> Path:
    return report_dir_for_run(execution_date, base_dir=base_dir) / "discovery_report.jsonl"


def channel_errors_path_for_run(execution_date: datetime, base_dir: str | Path = "data/reports") -> Path:
    return report_dir_for_run(execution_date, base_dir=base_dir) / "channel_errors.jsonl"


def extract_execution_date_from_snapshot_path(path: Path) -> datetime | None:
    dt_raw: str | None = None
    run_raw: str | None = None
    for part in path.parts:
        if part.startswith("dt="):
            dt_raw = part[3:]
        if part.startswith("run="):
            run_raw = part[4:]

    if not dt_raw or not run_raw or not run_raw.endswith("Z"):
        return None

    time_part = run_raw[:-1]
    if len(time_part) != 6:
        return None

    try:
        return datetime.strptime(f"{dt_raw} {time_part}", "%Y-%m-%d %H%M%S").replace(tzinfo=timezone.utc)
    except ValueError:
        return None


def list_snapshot_files(base_dir: str | Path = "data/snapshots") -> list[Path]:
    root = Path(base_dir)
    if not root.exists():
        return []
    return sorted(root.rglob("snapshots.jsonl.gz"))


def list_delta_files(base_dir: str | Path = "data/deltas") -> list[Path]:
    root = Path(base_dir)
    if not root.exists():
        return []
    return sorted(root.rglob("deltas.jsonl.gz"))
