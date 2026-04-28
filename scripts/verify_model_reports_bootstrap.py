from __future__ import annotations

import json
from pathlib import Path

REQUIRED_REPORTS = [
    "latest_model_leaderboard.csv",
    "latest_feature_importance.csv",
    "latest_feature_direction.csv",
    "latest_model_suite_report.html",
    "latest_content_driver_leaderboard.csv",
    "latest_content_driver_feature_importance.csv",
    "latest_content_driver_feature_direction.csv",
    "latest_content_driver_group_importance.csv",
    "latest_content_driver_report.html",
]


def main() -> int:
    root = Path("data/model_reports")
    missing = [name for name in REQUIRED_REPORTS if not (root / name).exists()]

    result: dict[str, object] = {
        "model_reports_dir": str(root),
        "present": sorted([name for name in REQUIRED_REPORTS if (root / name).exists()]),
        "missing": missing,
    }

    manifest_path = Path("site/data/site_manifest.json")
    if manifest_path.exists():
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
        warnings = manifest.get("warnings", [])
        model_report_warnings = [
            warning
            for warning in warnings
            if isinstance(warning, str) and "model_reports" in warning
        ]
        result["site_manifest_path"] = str(manifest_path)
        result["site_manifest_warnings_total"] = len(warnings)
        result["site_manifest_model_report_warnings"] = model_report_warnings
    else:
        result["site_manifest_path"] = str(manifest_path)
        result["site_manifest_missing"] = True

    print(json.dumps(result, ensure_ascii=False, indent=2))
    return 1 if missing else 0


if __name__ == "__main__":
    raise SystemExit(main())
