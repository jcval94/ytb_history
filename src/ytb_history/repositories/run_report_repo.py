"""Run report repository for per-run JSON/JSONL artifacts."""

from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path
from typing import Any

from ytb_history.domain.models import ChannelDiscoveryReport, QuotaReport, RunSummary
from ytb_history.storage.atomic_write import atomic_write_text
from ytb_history.storage.jsonl import read_jsonl, write_jsonl
from ytb_history.storage.partitioning import (
    channel_errors_path_for_run,
    discovery_report_path_for_run,
    quota_report_path_for_run,
    run_summary_path_for_run,
)


class RunReportRepo:
    def __init__(self, base_dir: str | Path = "data/reports") -> None:
        self.base_dir = Path(base_dir)

    def save_quota_report(self, execution_date: datetime, report: QuotaReport) -> Path:
        path = quota_report_path_for_run(execution_date, base_dir=self.base_dir)
        payload = json.dumps(report.to_dict(), ensure_ascii=False, indent=2) + "\n"
        atomic_write_text(path, payload)
        return path

    def load_quota_report(self, path: str | Path) -> dict[str, Any]:
        with Path(path).open("r", encoding="utf-8") as handle:
            return json.load(handle)

    def save_run_summary(self, execution_date: datetime, summary: RunSummary) -> Path:
        path = run_summary_path_for_run(execution_date, base_dir=self.base_dir)
        payload = json.dumps(summary.to_dict(), ensure_ascii=False, indent=2) + "\n"
        atomic_write_text(path, payload)
        return path

    def load_run_summary(self, path: str | Path) -> dict[str, Any]:
        with Path(path).open("r", encoding="utf-8") as handle:
            return json.load(handle)

    def save_discovery_report(self, execution_date: datetime, reports: list[ChannelDiscoveryReport]) -> Path:
        path = discovery_report_path_for_run(execution_date, base_dir=self.base_dir)
        write_jsonl(path, [report.to_dict() for report in reports])
        return path

    def load_discovery_report(self, path: str | Path) -> list[dict[str, Any]]:
        return read_jsonl(path)

    def save_channel_errors(self, execution_date: datetime, errors: list[dict[str, Any]]) -> Path:
        path = channel_errors_path_for_run(execution_date, base_dir=self.base_dir)
        write_jsonl(path, errors)
        return path

    def load_channel_errors(self, path: str | Path) -> list[dict[str, Any]]:
        return read_jsonl(path)


def save_run_report(_report: dict) -> None:
    """Backward-compatible scaffold function."""
    return None
