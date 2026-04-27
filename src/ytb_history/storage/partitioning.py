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


def snapshot_path_for_run(execution_date: datetime, base_dir: str | Path = "data/snapshots") -> Path:
    root = Path(base_dir)
    return root / make_dt_partition(execution_date) / f"run={make_run_id(execution_date)}" / "snapshots.jsonl.gz"


def delta_path_for_run(execution_date: datetime, base_dir: str | Path = "data/deltas") -> Path:
    root = Path(base_dir)
    return root / make_dt_partition(execution_date) / f"run={make_run_id(execution_date)}" / "deltas.jsonl.gz"


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
