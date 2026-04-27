"""Post-run validation helpers for generated artifacts."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any


def _load_json(path: Path) -> dict[str, Any] | None:
    if not path.exists():
        return None
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return None


def _latest_report_dir(reports_root: Path) -> Path | None:
    candidates: list[Path] = []
    for dt_dir in reports_root.glob("dt=*"):
        if not dt_dir.is_dir():
            continue
        for run_dir in dt_dir.glob("run=*"):
            if run_dir.is_dir():
                candidates.append(run_dir)

    if not candidates:
        return None

    return max(candidates, key=lambda p: (p.parent.name, p.name))


def validate_latest_run(*, data_dir: str | Path = "data") -> dict[str, Any]:
    data_root = Path(data_dir)
    reports_root = data_root / "reports"

    result: dict[str, Any] = {
        "status": "failed",
        "latest_report_dir": None,
        "run_summary_found": False,
        "quota_report_found": False,
        "snapshot_found": False,
        "delta_found": False,
        "discovery_report_found": False,
        "channel_errors_found": False,
        "quota_status": None,
        "total_estimated_units": None,
        "total_observed_units": None,
        "validations_passed": False,
        "warnings": [],
    }

    warnings: list[str] = result["warnings"]

    if not reports_root.exists():
        warnings.append(f"No existe el directorio de reportes: {reports_root}")
        return result

    latest_dir = _latest_report_dir(reports_root)
    if latest_dir is None:
        warnings.append(f"No hay corridas previas dentro de {reports_root}")
        return result

    result["latest_report_dir"] = str(latest_dir)

    run_summary_path = latest_dir / "run_summary.json"
    quota_report_path = latest_dir / "quota_report.json"
    discovery_report_path = latest_dir / "discovery_report.jsonl"
    channel_errors_path = latest_dir / "channel_errors.jsonl"

    run_summary = _load_json(run_summary_path)
    quota_report = _load_json(quota_report_path)

    result["run_summary_found"] = run_summary is not None
    result["quota_report_found"] = quota_report is not None
    result["discovery_report_found"] = discovery_report_path.exists()
    result["channel_errors_found"] = channel_errors_path.exists()

    if run_summary is None:
        warnings.append(f"Falta o es inválido: {run_summary_path}")
    if quota_report is None:
        warnings.append(f"Falta o es inválido: {quota_report_path}")
    if not result["discovery_report_found"]:
        warnings.append(f"Falta: {discovery_report_path}")
    if not result["channel_errors_found"]:
        warnings.append(f"Falta: {channel_errors_path}")

    run_status = str(run_summary.get("status")) if run_summary else None

    snapshot_required = run_status in {"success", "success_with_warnings"}

    if run_summary:
        snapshot_path_raw = run_summary.get("snapshot_path")
        delta_path_raw = run_summary.get("delta_path")

        if snapshot_path_raw:
            result["snapshot_found"] = Path(snapshot_path_raw).exists()
        if delta_path_raw:
            result["delta_found"] = Path(delta_path_raw).exists()

        if snapshot_required and not result["snapshot_found"]:
            warnings.append("status requiere snapshot_path existente, pero no se encontró snapshot")
        if snapshot_required and not result["delta_found"]:
            warnings.append("status requiere delta_path existente, pero no se encontró delta")

    if quota_report:
        result["quota_status"] = quota_report.get("limit_status")
        result["total_estimated_units"] = quota_report.get("total_estimated_units")
        result["total_observed_units"] = quota_report.get("total_observed_units")

        total_estimated = quota_report.get("total_estimated_units")
        operational_limit = quota_report.get("operational_limit")
        if isinstance(total_estimated, int) and isinstance(operational_limit, int):
            if total_estimated >= operational_limit:
                warnings.append(
                    "total_estimated_units está en o sobre operational_limit "
                    f"({total_estimated} >= {operational_limit})"
                )
    elif run_summary:
        result["quota_status"] = run_summary.get("quota_status")
        result["total_estimated_units"] = run_summary.get("estimated_quota_units")
        result["total_observed_units"] = run_summary.get("observed_quota_units")

    required_flags = [
        result["run_summary_found"],
        result["quota_report_found"],
        result["discovery_report_found"],
        result["channel_errors_found"],
    ]
    if snapshot_required:
        required_flags.extend([result["snapshot_found"], result["delta_found"]])

    result["validations_passed"] = all(bool(flag) for flag in required_flags)
    result["status"] = "success" if result["validations_passed"] else "failed"
    return result
