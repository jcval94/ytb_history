from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path

import pytest
import yaml

from ytb_history.services.operations_service import build_operations, load_operations_registry


def _write_text(path: Path, content: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content, encoding="utf-8")


def _registry(processes: list[dict]) -> dict:
    return {"schema_version": "operations_registry_v1", "processes": processes}


def _process(**overrides) -> dict:
    base = {
        "process_id": "sample_process",
        "name": "Sample Process",
        "domain": "analytics",
        "process_type": "cli",
        "cadence": "daily",
        "sla_hours": 36,
        "command": "python -m ytb_history.cli sample",
        "inputs": ["data/input.json"],
        "outputs": ["data/output.json"],
        "secrets_required": [],
        "expected_artifacts": [],
        "dashboard_tabs": ["Overview", "Operations"],
        "depends_on": [],
    }
    base.update(overrides)
    return base


def _write_registry(path: Path, processes: list[dict]) -> None:
    _write_text(path, yaml.safe_dump(_registry(processes), sort_keys=False))


def test_load_default_operations_registry_validates_processes() -> None:
    registry = load_operations_registry("config/operations.yaml")

    process_ids = {process["process_id"] for process in registry["processes"]}
    assert "cli_build_operations" in process_ids
    assert "github_monitor" in process_ids
    assert len(process_ids) == len(registry["processes"])


def test_load_operations_registry_rejects_missing_required_fields(tmp_path: Path) -> None:
    config_path = tmp_path / "operations.yaml"
    _write_text(config_path, yaml.safe_dump(_registry([{"process_id": "bad"}])))

    with pytest.raises(ValueError, match="missing required fields"):
        load_operations_registry(config_path)


def test_build_operations_derives_warning_status_and_impact_matrix(tmp_path: Path) -> None:
    data_dir = tmp_path / "data"
    config_path = tmp_path / "operations.yaml"
    generated_at = datetime.now(timezone.utc).isoformat()
    _write_text(
        data_dir / "reports" / "latest.json",
        json.dumps(
            {
                "status": "success",
                "generated_at": generated_at,
                "warnings": ["partial_channel_error"],
                "errors": [],
                "outputs": ["data/out.json"],
            },
            ensure_ascii=False,
        ),
    )
    _write_registry(
        config_path,
        [
            _process(
                expected_artifacts=[
                    {
                        "path": "reports/latest.json",
                        "type": "json",
                        "timestamp_field": "generated_at",
                        "required": True,
                    }
                ]
            )
        ],
    )

    summary = build_operations(data_dir=data_dir, config_path=config_path, repo_dir=tmp_path)

    assert summary["status"] == "success_with_warnings"
    process_status = json.loads((data_dir / "operations" / "latest_process_status.json").read_text(encoding="utf-8"))
    process = process_status["processes"][0]
    assert process["status"] == "success_with_warnings"
    assert process["warnings_count"] == 1

    impact_csv = (data_dir / "operations" / "dashboard_impact_matrix.csv").read_text(encoding="utf-8")
    assert "sample_process" in impact_csv
    assert "Operations" in impact_csv


def test_build_operations_marks_stale_artifact(tmp_path: Path) -> None:
    data_dir = tmp_path / "data"
    config_path = tmp_path / "operations.yaml"
    _write_text(
        data_dir / "reports" / "old.json",
        json.dumps({"status": "success", "generated_at": "2000-01-01T00:00:00+00:00", "warnings": []}),
    )
    _write_registry(
        config_path,
        [
            _process(
                sla_hours=1,
                expected_artifacts=[
                    {"path": "reports/old.json", "type": "json", "timestamp_field": "generated_at", "required": True}
                ],
            )
        ],
    )

    build_operations(data_dir=data_dir, config_path=config_path, repo_dir=tmp_path)
    process_status = json.loads((data_dir / "operations" / "latest_process_status.json").read_text(encoding="utf-8"))

    assert process_status["processes"][0]["status"] == "stale"
    assert process_status["processes"][0]["base_status"] == "success"


def test_build_operations_marks_missing_required_artifact_not_initialized(tmp_path: Path) -> None:
    data_dir = tmp_path / "data"
    config_path = tmp_path / "operations.yaml"
    _write_registry(
        config_path,
        [
            _process(
                expected_artifacts=[
                    {"path": "reports/missing.json", "type": "json", "timestamp_field": "generated_at", "required": True}
                ]
            )
        ],
    )

    build_operations(data_dir=data_dir, config_path=config_path, repo_dir=tmp_path)
    process_status = json.loads((data_dir / "operations" / "latest_process_status.json").read_text(encoding="utf-8"))

    assert process_status["processes"][0]["status"] == "not_initialized"
    assert process_status["processes"][0]["missing_required_artifacts"] == ["reports/missing.json"]


def test_build_operations_reads_json_with_utf8_bom(tmp_path: Path) -> None:
    data_dir = tmp_path / "data"
    config_path = tmp_path / "operations.yaml"
    payload = {
        "status": "success",
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "warnings": [],
        "errors": [],
    }
    artifact_path = data_dir / "reports" / "bom.json"
    artifact_path.parent.mkdir(parents=True, exist_ok=True)
    artifact_path.write_bytes(b"\xef\xbb\xbf" + json.dumps(payload).encode("utf-8"))
    _write_registry(
        config_path,
        [
            _process(
                expected_artifacts=[
                    {"path": "reports/bom.json", "type": "json", "timestamp_field": "generated_at", "required": True}
                ],
            )
        ],
    )

    build_operations(data_dir=data_dir, config_path=config_path, repo_dir=tmp_path)
    process_status = json.loads((data_dir / "operations" / "latest_process_status.json").read_text(encoding="utf-8"))

    process = process_status["processes"][0]
    assert process["status"] == "success"
    assert process["errors_count"] == 0


def test_build_operations_marks_missing_local_only_artifacts_skipped(tmp_path: Path) -> None:
    data_dir = tmp_path / "data"
    config_path = tmp_path / "operations.yaml"
    _write_registry(
        config_path,
        [
            _process(
                domain="local",
                observability_scope="local_only",
                expected_artifacts=[
                    {
                        "path": "build/local_automation/schedule_state.json",
                        "base": "repo",
                        "type": "json",
                        "timestamp_field": "updated_at",
                        "required": False,
                    }
                ],
            )
        ],
    )

    build_operations(data_dir=data_dir, config_path=config_path, repo_dir=tmp_path)
    process_status = json.loads((data_dir / "operations" / "latest_process_status.json").read_text(encoding="utf-8"))

    process = process_status["processes"][0]
    assert process["observability_scope"] == "local_only"
    assert process["status"] == "skipped"
    assert process["base_status"] == "skipped"
    assert process["missing_required_artifacts"] == []
    assert "Local-only artifacts" in process["status_detail"]


def test_build_operations_writes_latest_and_snapshot_outputs(tmp_path: Path) -> None:
    data_dir = tmp_path / "data"
    config_path = tmp_path / "operations.yaml"
    _write_registry(config_path, [_process(process_id="cli_build_operations", domain="operations")])

    summary = build_operations(data_dir=data_dir, config_path=config_path, repo_dir=tmp_path)

    assert (data_dir / "operations" / "latest_process_status.json").exists()
    assert (data_dir / "operations" / "process_catalog.json").exists()
    assert (data_dir / "operations" / "dashboard_impact_matrix.csv").exists()
    assert (data_dir / "operations" / "operation_summary.json").exists()
    assert Path(summary["snapshot_dir"]).exists()
    process_status = json.loads((data_dir / "operations" / "latest_process_status.json").read_text(encoding="utf-8"))
    assert process_status["processes"][0]["status"] == "success"
