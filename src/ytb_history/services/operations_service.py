"""Operations telemetry builder for repository processes."""

from __future__ import annotations

import csv
import io
import json
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import yaml

from ytb_history.storage.atomic_write import atomic_write_text

NORMALIZED_STATUSES = {
    "success",
    "success_with_warnings",
    "failed",
    "skipped",
    "stale",
    "not_initialized",
    "unknown",
}
SUCCESS_ALIASES = {"ok", "ready", "valid", "complete", "completed"}
WARNING_ALIASES = {"not_ready", "ready_with_notices", "warning", "warnings"}
SKIPPED_PREFIXES = ("skipped", "skip", "aborted_quota_guardrail")
FAILED_ALIASES = {"failed", "failure", "error", "errored", "cancelled", "canceled"}
REQUIRED_PROCESS_FIELDS = {
    "process_id",
    "name",
    "domain",
    "process_type",
    "cadence",
    "sla_hours",
    "command",
    "inputs",
    "outputs",
    "secrets_required",
    "expected_artifacts",
    "dashboard_tabs",
    "depends_on",
}
BLOCKED_SUMMARY_KEYS = {
    "stdout",
    "stderr",
    "command",
    "steps",
    "git",
    "top_alerts",
    "top_actions",
    "top_actions_this_week",
    "top_content_opportunities",
    "creative_packages_to_execute",
    "models",
    "champions",
    "evidence_json",
}


def load_operations_registry(config_path: str | Path = "config/operations.yaml") -> dict[str, Any]:
    """Load and validate the process registry."""

    registry_path = Path(config_path)
    try:
        raw = yaml.safe_load(registry_path.read_text(encoding="utf-8"))
    except FileNotFoundError as exc:
        raise ValueError(f"Operations registry not found: {registry_path}") from exc
    except yaml.YAMLError as exc:
        raise ValueError(f"Invalid YAML in operations registry {registry_path}: {exc}") from exc

    if not isinstance(raw, dict):
        raise ValueError(f"Operations registry must be a mapping: {registry_path}")
    processes = raw.get("processes")
    if not isinstance(processes, list) or not processes:
        raise ValueError("Operations registry must define a non-empty processes list")

    seen: set[str] = set()
    for index, process in enumerate(processes):
        if not isinstance(process, dict):
            raise ValueError(f"Process entry {index} must be a mapping")
        missing = sorted(REQUIRED_PROCESS_FIELDS - set(process))
        if missing:
            raise ValueError(f"Process entry {index} is missing required fields: {', '.join(missing)}")
        process_id = str(process["process_id"]).strip()
        if not process_id:
            raise ValueError(f"Process entry {index} has an empty process_id")
        if process_id in seen:
            raise ValueError(f"Duplicate process_id in operations registry: {process_id}")
        seen.add(process_id)
        _validate_list_field(process, "inputs")
        _validate_list_field(process, "outputs")
        _validate_list_field(process, "secrets_required")
        _validate_list_field(process, "expected_artifacts")
        _validate_list_field(process, "dashboard_tabs")
        _validate_list_field(process, "depends_on")
        _validate_sla(process, process_id)

    return raw


def build_operations(
    *,
    data_dir: str | Path = "data",
    config_path: str | Path = "config/operations.yaml",
    repo_dir: str | Path = ".",
) -> dict[str, Any]:
    """Build versioned operations telemetry from local, tracked artifacts."""

    registry = load_operations_registry(config_path)
    now = datetime.now(timezone.utc)
    generated_at = now.isoformat()
    data_root = Path(data_dir)
    repo_root = Path(repo_dir).resolve()
    operations_root = data_root / "operations"

    processes = [
        _evaluate_process(process, data_root=data_root, repo_root=repo_root, now=now)
        for process in registry["processes"]
    ]
    catalog = _build_process_catalog(generated_at, registry["processes"])
    process_status = {
        "schema_version": "operations_process_status_v1",
        "generated_at": generated_at,
        "source_registry": str(Path(config_path)),
        "processes": processes,
    }
    impact_rows = _build_impact_rows(processes)
    impact_csv = _impact_rows_to_csv(impact_rows)
    summary = _build_operation_summary(generated_at, processes, impact_rows)

    latest_paths = {
        "latest_process_status": operations_root / "latest_process_status.json",
        "process_catalog": operations_root / "process_catalog.json",
        "dashboard_impact_matrix": operations_root / "dashboard_impact_matrix.csv",
        "operation_summary": operations_root / "operation_summary.json",
    }
    snapshot_dir = _unique_snapshot_dir(operations_root, now)
    snapshot_paths = {
        "latest_process_status": snapshot_dir / "latest_process_status.json",
        "process_catalog": snapshot_dir / "process_catalog.json",
        "dashboard_impact_matrix": snapshot_dir / "dashboard_impact_matrix.csv",
        "operation_summary": snapshot_dir / "operation_summary.json",
    }

    _write_json(latest_paths["latest_process_status"], process_status)
    _write_json(latest_paths["process_catalog"], catalog)
    atomic_write_text(latest_paths["dashboard_impact_matrix"], impact_csv)
    _write_json(latest_paths["operation_summary"], summary)
    _write_json(snapshot_paths["latest_process_status"], process_status)
    _write_json(snapshot_paths["process_catalog"], catalog)
    atomic_write_text(snapshot_paths["dashboard_impact_matrix"], impact_csv)
    _write_json(snapshot_paths["operation_summary"], summary)

    non_success_count = summary["processes_total"] - summary["healthy_count"]
    return {
        "status": "success" if non_success_count == 0 else "success_with_warnings",
        "generated_at": generated_at,
        "operations_dir": str(operations_root),
        "snapshot_dir": str(snapshot_dir),
        "processes_total": summary["processes_total"],
        "status_counts": summary["status_counts"],
        "stale_count": summary["stale_count"],
        "failed_count": summary["failed_count"],
        "not_initialized_count": summary["not_initialized_count"],
        "unknown_count": summary["unknown_count"],
        "files_written": [str(path) for path in [*latest_paths.values(), *snapshot_paths.values()]],
        "warnings": summary["warnings"],
    }


def _validate_list_field(process: dict[str, Any], field: str) -> None:
    value = process[field]
    if not isinstance(value, list):
        raise ValueError(f"{process.get('process_id', '<unknown>')}.{field} must be a list")


def _validate_sla(process: dict[str, Any], process_id: str) -> None:
    try:
        value = float(process["sla_hours"])
    except (TypeError, ValueError) as exc:
        raise ValueError(f"{process_id}.sla_hours must be numeric") from exc
    if value <= 0:
        raise ValueError(f"{process_id}.sla_hours must be greater than zero")


def _evaluate_process(process: dict[str, Any], *, data_root: Path, repo_root: Path, now: datetime) -> dict[str, Any]:
    artifact_results = [
        _evaluate_artifact(artifact, data_root=data_root, repo_root=repo_root, now=now)
        for artifact in process.get("expected_artifacts", [])
    ]
    observability_scope = str(process.get("observability_scope", "")).strip()
    local_only_missing = observability_scope == "local_only" and bool(artifact_results) and not any(
        item.get("exists") for item in artifact_results
    )
    workflow_file = process.get("workflow_file")
    configured = True
    configuration_warnings: list[str] = []
    if workflow_file:
        workflow_path = repo_root / str(workflow_file)
        configured = workflow_path.exists()
        if not configured:
            configuration_warnings.append(f"workflow_missing:{workflow_file}")

    last_run_at = _latest_timestamp([item.get("observed_at") for item in artifact_results])
    age_hours = _age_hours(last_run_at, now)
    base_status = "skipped" if local_only_missing else _derive_base_status(artifact_results, configured=configured)
    is_stale = last_run_at is not None and age_hours is not None and age_hours > float(process["sla_hours"])
    status = "stale" if is_stale else base_status

    warnings_count = sum(int(item.get("warnings_count", 0)) for item in artifact_results) + len(configuration_warnings)
    errors_count = sum(int(item.get("errors_count", 0)) for item in artifact_results)
    failure_count = sum(int(item.get("failure_count", 0)) for item in artifact_results)
    warning_samples = _collect_samples(artifact_results, "warning_samples", configuration_warnings)
    error_samples = _collect_samples(artifact_results, "error_samples", [])
    existing_artifacts = [item for item in artifact_results if item.get("exists")]

    if process["process_id"] == "cli_build_operations":
        last_run_at = now
        age_hours = 0.0
        base_status = "success"
        status = "success"
        is_stale = False

    return {
        "process_id": process["process_id"],
        "name": process["name"],
        "domain": process["domain"],
        "process_type": process["process_type"],
        "cadence": process["cadence"],
        "sla_hours": process["sla_hours"],
        "command": process["command"],
        "workflow_file": workflow_file or "",
        "observability_scope": observability_scope,
        "configured": configured,
        "status": status,
        "base_status": base_status,
        "status_detail": _status_detail(
            status,
            base_status,
            artifact_results,
            configuration_warnings,
            local_only_missing=local_only_missing,
        ),
        "last_run_at": last_run_at.isoformat() if last_run_at else None,
        "age_hours": round(age_hours, 2) if age_hours is not None else None,
        "is_stale": is_stale,
        "expected_artifacts_count": len(artifact_results),
        "existing_artifacts_count": len(existing_artifacts),
        "missing_required_artifacts": [
            item["path"] for item in artifact_results if item.get("required") and not item.get("exists")
        ],
        "artifacts": artifact_results,
        "warnings_count": warnings_count,
        "errors_count": errors_count,
        "failure_count": failure_count,
        "warning_samples": warning_samples,
        "error_samples": error_samples,
        "inputs": process.get("inputs", []),
        "outputs": process.get("outputs", []),
        "secrets_required": process.get("secrets_required", []),
        "dashboard_tabs": process.get("dashboard_tabs", []),
        "depends_on": process.get("depends_on", []),
    }


def _evaluate_artifact(artifact: Any, *, data_root: Path, repo_root: Path, now: datetime) -> dict[str, Any]:
    spec = _normalize_artifact_spec(artifact)
    base = repo_root if spec["base"] == "repo" else data_root
    matches = sorted(base.glob(spec["path"]))
    latest = _latest_artifact_summary(matches, spec=spec, base=base, now=now)
    if latest is None:
        return {
            "path": spec["path"],
            "base": spec["base"],
            "type": spec["type"],
            "required": spec["required"],
            "exists": False,
            "matched_count": 0,
            "observed_at": None,
            "status": "not_initialized",
            "summary": {},
            "warnings_count": 0,
            "errors_count": 0,
            "failure_count": 0,
            "warning_samples": [],
            "error_samples": [],
        }

    latest.update(
        {
            "path": spec["path"],
            "base": spec["base"],
            "type": spec["type"],
            "required": spec["required"],
            "exists": True,
            "matched_count": len(matches),
        }
    )
    return latest


def _normalize_artifact_spec(artifact: Any) -> dict[str, Any]:
    if isinstance(artifact, str):
        return {"path": artifact, "base": "data", "type": _infer_artifact_type(artifact), "required": True}
    if not isinstance(artifact, dict):
        raise ValueError("expected_artifacts entries must be strings or mappings")
    path = str(artifact.get("path", "")).strip()
    if not path:
        raise ValueError("expected artifact is missing path")
    return {
        "path": path,
        "base": str(artifact.get("base", "data")),
        "type": str(artifact.get("type") or _infer_artifact_type(path)),
        "required": bool(artifact.get("required", True)),
        "timestamp_field": artifact.get("timestamp_field"),
    }


def _infer_artifact_type(path: str) -> str:
    suffix = Path(path).suffix.lower()
    if suffix == ".json":
        return "json"
    if suffix == ".csv":
        return "csv"
    return "text"


def _latest_artifact_summary(matches: list[Path], *, spec: dict[str, Any], base: Path, now: datetime) -> dict[str, Any] | None:
    summaries = [_artifact_summary(path, spec=spec, base=base, now=now) for path in matches if path.is_file()]
    summaries = [item for item in summaries if item is not None]
    if not summaries:
        return None
    return max(summaries, key=lambda item: item.get("observed_at") or datetime.fromtimestamp(0, tz=timezone.utc))


def _artifact_summary(path: Path, *, spec: dict[str, Any], base: Path, now: datetime) -> dict[str, Any] | None:
    try:
        relative_path = path.relative_to(base).as_posix()
    except ValueError:
        relative_path = path.as_posix()

    observed_at = datetime.fromtimestamp(path.stat().st_mtime, tz=timezone.utc)
    status = "success"
    summary: dict[str, Any] = {"size_bytes": path.stat().st_size}
    warnings_count = 0
    errors_count = 0
    failure_count = 0
    warning_samples: list[str] = []
    error_samples: list[str] = []

    if spec["type"] == "json":
        try:
            payload = json.loads(path.read_text(encoding="utf-8-sig"))
        except json.JSONDecodeError as exc:
            return {
                "resolved_path": relative_path,
                "observed_at": observed_at,
                "status": "failed",
                "summary": {"json_error": str(exc)},
                "warnings_count": 0,
                "errors_count": 1,
                "failure_count": 1,
                "warning_samples": [],
                "error_samples": ["invalid_json"],
            }
        if not isinstance(payload, dict):
            return {
                "resolved_path": relative_path,
                "observed_at": observed_at,
                "status": "failed",
                "summary": {"json_error": "payload_not_object"},
                "warnings_count": 0,
                "errors_count": 1,
                "failure_count": 1,
                "warning_samples": [],
                "error_samples": ["payload_not_object"],
            }
        observed_at = _extract_timestamp(payload, spec, fallback=observed_at)
        warnings = payload.get("warnings", [])
        errors = payload.get("errors", [])
        warnings_count = len(warnings) if isinstance(warnings, list) else 0
        errors_count = len(errors) if isinstance(errors, list) else 0
        failure_count = _failure_count(payload)
        warning_samples = [str(item) for item in warnings[:5]] if isinstance(warnings, list) else []
        error_samples = [str(item) for item in errors[:5]] if isinstance(errors, list) else []
        status = _normalize_status(payload.get("status"), warnings_count=warnings_count, errors_count=errors_count, failure_count=failure_count)
        summary = _sanitize_payload_summary(payload)
    elif spec["type"] == "csv":
        row_count = _csv_row_count(path)
        summary.update({"row_count": row_count})
        status = "success" if row_count >= 0 else "failed"
    else:
        summary.update({"line_count": _line_count(path)})
        status = "success"

    age_hours = (now - observed_at).total_seconds() / 3600
    return {
        "resolved_path": relative_path,
        "observed_at": observed_at,
        "observed_at_iso": observed_at.isoformat(),
        "age_hours": round(age_hours, 2),
        "status": status,
        "summary": summary,
        "warnings_count": warnings_count,
        "errors_count": errors_count,
        "failure_count": failure_count,
        "warning_samples": warning_samples,
        "error_samples": error_samples,
    }


def _extract_timestamp(payload: dict[str, Any], spec: dict[str, Any], *, fallback: datetime) -> datetime:
    fields = [spec.get("timestamp_field"), "generated_at", "execution_date", "created_at", "updated_at"]
    for field in [item for item in fields if item]:
        value = payload.get(str(field))
        parsed = _parse_datetime(value)
        if parsed is not None:
            return parsed
    return fallback


def _parse_datetime(value: Any) -> datetime | None:
    if not value:
        return None
    text = str(value).strip()
    if not text:
        return None
    if len(text) == 10 and text[4] == "-" and text[7] == "-":
        text = f"{text}T00:00:00+00:00"
    if text.endswith("Z"):
        text = f"{text[:-1]}+00:00"
    try:
        parsed = datetime.fromisoformat(text)
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _normalize_status(raw_status: Any, *, warnings_count: int, errors_count: int, failure_count: int) -> str:
    normalized = str(raw_status or "").strip().lower()
    if normalized in NORMALIZED_STATUSES:
        status = normalized
    elif normalized in SUCCESS_ALIASES:
        status = "success"
    elif normalized in WARNING_ALIASES:
        status = "success_with_warnings"
    elif normalized in FAILED_ALIASES:
        status = "failed"
    elif any(normalized.startswith(prefix) for prefix in SKIPPED_PREFIXES):
        status = "skipped"
    elif not normalized:
        status = "success"
    else:
        status = "unknown"

    if status == "success" and (warnings_count or errors_count or failure_count):
        return "success_with_warnings"
    return status


def _sanitize_payload_summary(payload: dict[str, Any]) -> dict[str, Any]:
    summary: dict[str, Any] = {}
    for key, value in payload.items():
        if key in BLOCKED_SUMMARY_KEYS:
            continue
        if isinstance(value, (str, int, float, bool)) or value is None:
            summary[key] = _trim_scalar(value)
        elif isinstance(value, list):
            summary[f"{key}_count"] = len(value)
        elif isinstance(value, dict):
            scalar_items = {
                str(nested_key): _trim_scalar(nested_value)
                for nested_key, nested_value in value.items()
                if isinstance(nested_value, (str, int, float, bool)) or nested_value is None
            }
            if scalar_items and len(scalar_items) <= 8:
                summary[key] = scalar_items
            else:
                summary[f"{key}_keys_count"] = len(value)
    return summary


def _trim_scalar(value: Any) -> Any:
    if isinstance(value, str) and len(value) > 240:
        return f"{value[:237]}..."
    return value


def _failure_count(payload: dict[str, Any]) -> int:
    total = 0
    for key in ("failed", "failed_count", "failure_count", "failed_audio_download", "channels_failed"):
        value = payload.get(key)
        if isinstance(value, bool):
            continue
        if isinstance(value, (int, float)):
            total += int(value)
    failed_details = payload.get("failed_details")
    if isinstance(failed_details, list):
        total += len(failed_details)
    return total


def _csv_row_count(path: Path) -> int:
    try:
        with path.open("r", encoding="utf-8", newline="") as handle:
            reader = csv.reader(handle)
            rows = list(reader)
    except UnicodeDecodeError:
        return -1
    return max(0, len(rows) - 1)


def _line_count(path: Path) -> int:
    try:
        with path.open("r", encoding="utf-8") as handle:
            return sum(1 for _ in handle)
    except UnicodeDecodeError:
        return 0


def _derive_base_status(artifact_results: list[dict[str, Any]], *, configured: bool) -> str:
    if not configured:
        return "unknown"
    if not artifact_results:
        return "unknown"
    required_missing = [item for item in artifact_results if item.get("required") and not item.get("exists")]
    if required_missing:
        return "not_initialized"
    existing_statuses = [str(item.get("status", "unknown")) for item in artifact_results if item.get("exists")]
    if not existing_statuses:
        return "not_initialized"
    if "failed" in existing_statuses:
        return "failed"
    if "success_with_warnings" in existing_statuses:
        return "success_with_warnings"
    if all(status == "skipped" for status in existing_statuses):
        return "skipped"
    if "success" in existing_statuses:
        return "success"
    if "unknown" in existing_statuses:
        return "unknown"
    return existing_statuses[0]


def _status_detail(
    status: str,
    base_status: str,
    artifact_results: list[dict[str, Any]],
    configuration_warnings: list[str],
    *,
    local_only_missing: bool = False,
) -> str:
    if configuration_warnings:
        return "; ".join(configuration_warnings)
    if local_only_missing:
        return "Local-only artifacts are unavailable in this environment; process is skipped for remote observability."
    if status == "stale":
        return f"Last successful artifact is older than SLA; base status was {base_status}."
    missing = [item["path"] for item in artifact_results if item.get("required") and not item.get("exists")]
    if missing:
        return f"Missing required artifacts: {', '.join(missing[:3])}"
    if status == "unknown":
        return "Configured but no durable execution artifact is available yet."
    if status == "success_with_warnings":
        return "Latest artifact exists but reports warnings, errors, or partial failures."
    return "Latest durable artifact is within expected state."


def _latest_timestamp(values: list[Any]) -> datetime | None:
    parsed = [value for value in values if isinstance(value, datetime)]
    if not parsed:
        return None
    return max(parsed)


def _age_hours(value: datetime | None, now: datetime) -> float | None:
    if value is None:
        return None
    return (now - value).total_seconds() / 3600


def _collect_samples(artifact_results: list[dict[str, Any]], field: str, seed: list[str]) -> list[str]:
    samples = list(seed)
    for item in artifact_results:
        values = item.get(field, [])
        if isinstance(values, list):
            samples.extend(str(value) for value in values)
    return samples[:8]


def _build_process_catalog(generated_at: str, processes: list[dict[str, Any]]) -> dict[str, Any]:
    return {
        "schema_version": "operations_process_catalog_v1",
        "generated_at": generated_at,
        "processes": [
            {
                "process_id": process["process_id"],
                "name": process["name"],
                "domain": process["domain"],
                "process_type": process["process_type"],
                "cadence": process["cadence"],
                "sla_hours": process["sla_hours"],
                "command": process["command"],
                "workflow_file": process.get("workflow_file", ""),
                "observability_scope": process.get("observability_scope", ""),
                "inputs": process.get("inputs", []),
                "outputs": process.get("outputs", []),
                "secrets_required": process.get("secrets_required", []),
                "dashboard_tabs": process.get("dashboard_tabs", []),
                "depends_on": process.get("depends_on", []),
                "expected_artifacts": process.get("expected_artifacts", []),
            }
            for process in processes
        ],
    }


def _build_impact_rows(processes: list[dict[str, Any]]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for process in processes:
        for tab in process.get("dashboard_tabs", []):
            rows.append(
                {
                    "process_id": process["process_id"],
                    "name": process["name"],
                    "domain": process["domain"],
                    "process_type": process["process_type"],
                    "cadence": process["cadence"],
                    "dashboard_tab": tab,
                    "impact_type": _impact_type(process, tab),
                    "status": process["status"],
                    "last_run_at": process["last_run_at"] or "",
                    "is_stale": str(bool(process["is_stale"])),
                }
            )
    return rows


def _impact_type(process: dict[str, Any], tab: str) -> str:
    if tab == "Operations":
        return "observability"
    domain = process.get("domain")
    if domain in {"ingestion", "analytics", "validation"}:
        return "data_source"
    if domain in {"intelligence", "creative", "reporting", "ml", "transcription"}:
        return "derived_signal"
    if domain == "publishing":
        return "publication"
    return "workflow"


def _impact_rows_to_csv(rows: list[dict[str, Any]]) -> str:
    columns = [
        "process_id",
        "name",
        "domain",
        "process_type",
        "cadence",
        "dashboard_tab",
        "impact_type",
        "status",
        "last_run_at",
        "is_stale",
    ]
    output = io.StringIO()
    writer = csv.DictWriter(output, fieldnames=columns, lineterminator="\n")
    writer.writeheader()
    writer.writerows(rows)
    return output.getvalue()


def _build_operation_summary(generated_at: str, processes: list[dict[str, Any]], impact_rows: list[dict[str, Any]]) -> dict[str, Any]:
    status_counts = dict(Counter(process["status"] for process in processes))
    domains = dict(Counter(process["domain"] for process in processes))
    stale = [process["process_id"] for process in processes if process["status"] == "stale"]
    failed = [process["process_id"] for process in processes if process["status"] == "failed"]
    warning = [process["process_id"] for process in processes if process["status"] == "success_with_warnings"]
    not_initialized = [process["process_id"] for process in processes if process["status"] == "not_initialized"]
    unknown = [process["process_id"] for process in processes if process["status"] == "unknown"]
    tabs = sorted({row["dashboard_tab"] for row in impact_rows})
    warnings = []
    if stale:
        warnings.append(f"stale_processes:{len(stale)}")
    if failed:
        warnings.append(f"failed_processes:{len(failed)}")
    if not_initialized:
        warnings.append(f"not_initialized_processes:{len(not_initialized)}")

    return {
        "schema_version": "operations_summary_v1",
        "generated_at": generated_at,
        "processes_total": len(processes),
        "status_counts": status_counts,
        "domain_counts": domains,
        "dashboard_tabs": tabs,
        "dashboard_impact_rows": len(impact_rows),
        "healthy_count": status_counts.get("success", 0),
        "warning_count": status_counts.get("success_with_warnings", 0),
        "stale_count": len(stale),
        "failed_count": len(failed),
        "not_initialized_count": len(not_initialized),
        "unknown_count": len(unknown),
        "action_required_count": len(stale) + len(failed),
        "stale_processes": stale,
        "failed_processes": failed,
        "warning_processes": warning,
        "not_initialized_processes": not_initialized,
        "unknown_processes": unknown,
        "warnings": warnings,
    }


def _unique_snapshot_dir(operations_root: Path, generated_at: datetime) -> Path:
    date_part = generated_at.strftime("%Y-%m-%d")
    run_part = generated_at.strftime("%H%M%SZ")
    base = operations_root / "runs" / f"dt={date_part}" / f"run={run_part}"
    if not base.exists():
        return base
    for index in range(1, 1000):
        candidate = operations_root / "runs" / f"dt={date_part}" / f"run={run_part}-{index:03d}"
        if not candidate.exists():
            return candidate
    raise RuntimeError(f"Could not allocate unique operations snapshot directory under {operations_root}")


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    atomic_write_text(path, json.dumps(_json_ready(payload), ensure_ascii=False, indent=2) + "\n")


def _json_ready(value: Any) -> Any:
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, dict):
        return {key: _json_ready(item) for key, item in value.items()}
    if isinstance(value, list):
        return [_json_ready(item) for item in value]
    return value
