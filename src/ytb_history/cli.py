"""CLI entrypoint for ytb_history."""

from __future__ import annotations

import argparse
import json

from ytb_history.orchestrator import run_dry_run, run_pipeline
from ytb_history.services.validation_service import validate_latest_run


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="ytb_history")
    sub = parser.add_subparsers(dest="command", required=True)

    run_parser = sub.add_parser("run", help="Run daily pipeline")
    run_parser.add_argument("--settings", default="config/settings.yaml")
    run_parser.add_argument("--data-dir", default="data")

    dry_run_parser = sub.add_parser("dry-run", help="Estimate quota without API calls")
    dry_run_parser.add_argument("--settings", default="config/settings.yaml")
    dry_run_parser.add_argument("--data-dir", default="data")

    validate_parser = sub.add_parser("validate-latest", help="Validate latest run artifacts")
    validate_parser.add_argument("--data-dir", default="data")
    return parser


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()

    if args.command == "run":
        summary = run_pipeline(settings_path=args.settings, data_dir=args.data_dir)
        print(json.dumps(summary, ensure_ascii=False, indent=2))
        return 0

    if args.command == "dry-run":
        summary = run_dry_run(settings_path=args.settings, data_dir=args.data_dir)
        print(json.dumps(summary, ensure_ascii=False, indent=2))
        return 0

    if args.command == "validate-latest":
        summary = validate_latest_run(data_dir=args.data_dir)
        print(json.dumps(summary, ensure_ascii=False, indent=2))
        return 0

    parser.print_help()
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
