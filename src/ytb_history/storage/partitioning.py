"""Path partitioning helpers for immutable run outputs."""

from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path


def _as_aware(execution_date: datetime) -> datetime:
    if execution_date.tzinfo is None:
        local_tz = datetime.now().astimezone().tzinfo
        if local_tz is None:
            return execution_date.replace(tzinfo=timezone.utc)
        return execution_date.replace(tzinfo=local_tz)
    return execution_date


def _as_utc(execution_date: datetime) -> datetime:
    return _as_aware(execution_date).astimezone(timezone.utc)


def _as_partition_timezone(execution_date: datetime) -> datetime:
    return _as_aware(execution_date)


def _format_offset(offset: str) -> str:
    if offset in ("+0000", "-0000"):
        return "Z"
    return offset


def make_run_id(execution_date: datetime) -> str:
    execution_date_local = _as_partition_timezone(execution_date)
    return execution_date_local.strftime("%H%M%S") + _format_offset(execution_date_local.strftime("%z"))


def make_dt_partition(execution_date: datetime) -> str:
    execution_date_local = _as_partition_timezone(execution_date)
    return f"dt={execution_date_local.strftime('%Y-%m-%d')}"


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


def export_dir_for_run(execution_date: datetime, base_dir: str | Path = "data/exports") -> Path:
    return _run_dir(execution_date, base_dir)


def latest_snapshots_csv_path_for_run(execution_date: datetime, base_dir: str | Path = "data/exports") -> Path:
    return export_dir_for_run(execution_date, base_dir=base_dir) / "latest_snapshots.csv"


def latest_deltas_csv_path_for_run(execution_date: datetime, base_dir: str | Path = "data/exports") -> Path:
    return export_dir_for_run(execution_date, base_dir=base_dir) / "latest_deltas.csv"


def growth_summary_csv_path_for_run(execution_date: datetime, base_dir: str | Path = "data/exports") -> Path:
    return export_dir_for_run(execution_date, base_dir=base_dir) / "video_growth_summary.csv"


def export_summary_path_for_run(execution_date: datetime, base_dir: str | Path = "data/exports") -> Path:
    return export_dir_for_run(execution_date, base_dir=base_dir) / "export_summary.json"


def _parse_legacy_run_id(dt_raw: str, run_raw: str) -> datetime | None:
    if not run_raw.endswith("Z"):
        return None
    time_part = run_raw[:-1]
    if len(time_part) != 6:
        return None
    try:
        return datetime.strptime(f"{dt_raw} {time_part}", "%Y-%m-%d %H%M%S").replace(tzinfo=timezone.utc)
    except ValueError:
        return None


def _parse_offset_run_id(dt_raw: str, run_raw: str) -> datetime | None:
    if len(run_raw) != 11:
        return None
    hhmmss = run_raw[:6]
    offset = run_raw[6:]
    if offset[:1] not in {"+", "-"}:
        return None
    try:
        return datetime.strptime(f"{dt_raw} {hhmmss}{offset}", "%Y-%m-%d %H%M%S%z")
    except ValueError:
        return None


def parse_run_datetime_from_path(path: Path) -> datetime | None:
    dt_raw: str | None = None
    run_raw: str | None = None
    for part in path.parts:
        if part.startswith("dt="):
            dt_raw = part[3:]
        if part.startswith("run="):
            run_raw = part[4:]

    if not dt_raw or not run_raw:
        return None

    return _parse_legacy_run_id(dt_raw, run_raw) or _parse_offset_run_id(dt_raw, run_raw)


def extract_execution_date_from_snapshot_path(path: Path) -> datetime | None:
    return parse_run_datetime_from_path(path)


def _normalize_run_id(run_raw: str) -> str:
    if run_raw.endswith("Z"):
        return run_raw[:-1] + "+0000"
    return run_raw


def _run_sort_key(path: Path) -> tuple[str, str]:
    dt_raw = ""
    run_raw = ""
    for part in path.parts:
        if part.startswith("dt="):
            dt_raw = part[3:]
        if part.startswith("run="):
            run_raw = _normalize_run_id(part[4:])
    return dt_raw, run_raw


def is_run_before(path: Path, execution_date: datetime) -> bool:
    target_dt = make_dt_partition(execution_date)[3:]
    target_run = _normalize_run_id(make_run_id(execution_date))
    return _run_sort_key(path) < (target_dt, target_run)


def latest_run_before(paths: list[Path], execution_date: datetime) -> Path | None:
    candidates = [path for path in paths if is_run_before(path, execution_date)]
    if not candidates:
        return None
    return sorted(candidates, key=_run_sort_key)[-1]


def list_snapshot_files(base_dir: str | Path = "data/snapshots") -> list[Path]:
    root = Path(base_dir)
    if not root.exists():
        return []
    return sorted(root.rglob("snapshots.jsonl.gz"), key=_run_sort_key)


def list_delta_files(base_dir: str | Path = "data/deltas") -> list[Path]:
    root = Path(base_dir)
    if not root.exists():
        return []
    return sorted(root.rglob("deltas.jsonl.gz"), key=_run_sort_key)
