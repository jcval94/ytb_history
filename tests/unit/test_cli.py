from __future__ import annotations

import json

from ytb_history import cli


def test_cli_run_calls_run_pipeline_once(monkeypatch, capsys) -> None:
    calls: list[dict] = []

    def _fake_run_pipeline(**kwargs):
        calls.append(kwargs)
        return {"status": "success", "channels_total": 1}

    monkeypatch.setattr(cli, "run_pipeline", _fake_run_pipeline)
    monkeypatch.setattr(
        "sys.argv",
        [
            "ytb_history",
            "run",
            "--settings",
            "custom/settings.yaml",
            "--data-dir",
            "custom/data",
        ],
    )

    code = cli.main()
    out = capsys.readouterr().out

    assert code == 0
    assert len(calls) == 1
    assert calls[0] == {
        "settings_path": "custom/settings.yaml",
        "data_dir": "custom/data",
    }
    assert json.loads(out)["status"] == "success"


def test_cli_dry_run_calls_run_dry_run_once(monkeypatch, capsys) -> None:
    calls: list[dict] = []

    def _fake_run_dry_run(**kwargs):
        calls.append(kwargs)
        return {"total_estimated_units": 100, "should_abort": False}

    monkeypatch.setattr(cli, "run_dry_run", _fake_run_dry_run)
    monkeypatch.setattr(
        "sys.argv",
        [
            "ytb_history",
            "dry-run",
            "--settings",
            "custom/settings.yaml",
            "--data-dir",
            "custom/data",
        ],
    )

    code = cli.main()
    out = capsys.readouterr().out

    assert code == 0
    assert len(calls) == 1
    assert calls[0] == {
        "settings_path": "custom/settings.yaml",
        "data_dir": "custom/data",
    }
    assert json.loads(out)["total_estimated_units"] == 100


def test_build_parser_has_exact_subcommands() -> None:
    parser = cli.build_parser()
    subcommands: set[str] = set()

    for action in parser._actions:
        if hasattr(action, "choices") and action.choices:
            subcommands = set(action.choices.keys())
            break

    assert subcommands == {"run", "dry-run"}
