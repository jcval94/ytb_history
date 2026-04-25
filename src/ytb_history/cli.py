"""CLI entrypoint for ytb_history."""

from __future__ import annotations

import argparse
import json

from ytb_history.orchestrator import run_pipeline


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="ytb_history")
    sub = parser.add_subparsers(dest="command", required=True)
    sub.add_parser("run", help="Run scaffold pipeline")
    return parser


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()

    if args.command == "run":
        summary = run_pipeline()
        print(json.dumps(summary, ensure_ascii=False, indent=2))
        return 0

    parser.print_help()
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
