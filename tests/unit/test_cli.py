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


def test_cli_validate_latest_prints_json(monkeypatch, capsys) -> None:
    monkeypatch.setattr(
        cli,
        "validate_latest_run",
        lambda **kwargs: {"status": "success", "latest_report_dir": kwargs["data_dir"]},
    )
    monkeypatch.setattr("sys.argv", ["ytb_history", "validate-latest", "--data-dir", "custom/data"])

    code = cli.main()
    out = capsys.readouterr().out

    assert code == 0
    assert json.loads(out) == {"status": "success", "latest_report_dir": "custom/data"}


def test_cli_validate_latest_does_not_call_api_flows(monkeypatch, capsys) -> None:
    def _boom(**_kwargs):
        raise AssertionError("API flow should not be called")

    monkeypatch.setattr(cli, "run_pipeline", _boom)
    monkeypatch.setattr(cli, "run_dry_run", _boom)
    monkeypatch.setattr(cli, "validate_latest_run", lambda **_kwargs: {"status": "failed"})
    monkeypatch.setattr("sys.argv", ["ytb_history", "validate-latest"])

    code = cli.main()
    out = capsys.readouterr().out

    assert code == 0
    assert json.loads(out)["status"] == "failed"


def test_cli_export_latest_prints_json(monkeypatch, capsys) -> None:
    monkeypatch.setattr(
        cli,
        "export_latest_run",
        lambda **kwargs: {"status": "success", "export_dir": kwargs["data_dir"]},
    )
    monkeypatch.setattr("sys.argv", ["ytb_history", "export-latest", "--data-dir", "custom/data"])

    code = cli.main()
    out = capsys.readouterr().out

    assert code == 0
    assert json.loads(out) == {"status": "success", "export_dir": "custom/data"}


def test_cli_export_latest_does_not_call_api_flows(monkeypatch, capsys) -> None:
    def _boom(**_kwargs):
        raise AssertionError("API flow should not be called")

    monkeypatch.setattr(cli, "run_pipeline", _boom)
    monkeypatch.setattr(cli, "run_dry_run", _boom)
    monkeypatch.setattr(cli, "export_latest_run", lambda **_kwargs: {"status": "success"})
    monkeypatch.setattr("sys.argv", ["ytb_history", "export-latest"])

    code = cli.main()
    out = capsys.readouterr().out

    assert code == 0
    assert json.loads(out)["status"] == "success"




def test_cli_build_analytics_prints_json(monkeypatch, capsys) -> None:
    monkeypatch.setattr(
        cli,
        "build_analytics",
        lambda **kwargs: {"status": "success", "analytics_dir": kwargs["data_dir"]},
    )
    monkeypatch.setattr("sys.argv", ["ytb_history", "build-analytics", "--data-dir", "custom/data"])

    code = cli.main()
    out = capsys.readouterr().out

    assert code == 0
    assert json.loads(out) == {"status": "success", "analytics_dir": "custom/data"}


def test_cli_build_analytics_does_not_call_api_flows(monkeypatch, capsys) -> None:
    def _boom(**_kwargs):
        raise AssertionError("API flow should not be called")

    monkeypatch.setattr(cli, "run_pipeline", _boom)
    monkeypatch.setattr(cli, "run_dry_run", _boom)
    monkeypatch.setattr(cli, "build_analytics", lambda **_kwargs: {"status": "success"})
    monkeypatch.setattr("sys.argv", ["ytb_history", "build-analytics"])

    code = cli.main()
    out = capsys.readouterr().out

    assert code == 0
    assert json.loads(out)["status"] == "success"

def test_build_parser_has_exact_subcommands() -> None:
    parser = cli.build_parser()
    subcommands: set[str] = set()

    for action in parser._actions:
        if hasattr(action, "choices") and action.choices:
            subcommands = set(action.choices.keys())
            break

    assert subcommands == {"run", "dry-run", "validate-latest", "export-latest", "build-analytics", "build-pages-dashboard", "generate-alerts", "build-decision-layer"}


def test_cli_build_pages_dashboard_prints_json(monkeypatch, capsys) -> None:
    monkeypatch.setattr(
        cli,
        "build_pages_dashboard",
        lambda **kwargs: {
            "status": "success",
            "site_dir": kwargs["site_dir"],
            "row_counts": {},
            "warnings": [],
            "files_written": [],
        },
    )
    monkeypatch.setattr(
        "sys.argv",
        ["ytb_history", "build-pages-dashboard", "--data-dir", "custom/data", "--site-dir", "custom/site"],
    )

    code = cli.main()
    out = capsys.readouterr().out

    assert code == 0
    assert json.loads(out)["site_dir"] == "custom/site"


def test_cli_build_pages_dashboard_does_not_call_api_flows(monkeypatch, capsys) -> None:
    def _boom(**_kwargs):
        raise AssertionError("API flow should not be called")

    monkeypatch.setattr(cli, "run_pipeline", _boom)
    monkeypatch.setattr(cli, "run_dry_run", _boom)
    monkeypatch.setattr(cli, "build_pages_dashboard", lambda **_kwargs: {"status": "success"})
    monkeypatch.setattr("sys.argv", ["ytb_history", "build-pages-dashboard"])

    code = cli.main()
    out = capsys.readouterr().out

    assert code == 0
    assert json.loads(out)["status"] == "success"


def test_cli_generate_alerts_prints_json(monkeypatch, capsys) -> None:
    monkeypatch.setattr(
        cli,
        "generate_alerts",
        lambda **kwargs: {"status": "success", "signals_dir": kwargs["data_dir"], "total_alerts": 3},
    )
    monkeypatch.setattr("sys.argv", ["ytb_history", "generate-alerts", "--data-dir", "custom/data"])

    code = cli.main()
    out = capsys.readouterr().out

    assert code == 0
    assert json.loads(out)["signals_dir"] == "custom/data"


def test_cli_generate_alerts_does_not_call_api_flows(monkeypatch, capsys) -> None:
    def _boom(**_kwargs):
        raise AssertionError("API flow should not be called")

    monkeypatch.setattr(cli, "run_pipeline", _boom)
    monkeypatch.setattr(cli, "run_dry_run", _boom)
    monkeypatch.setattr(cli, "generate_alerts", lambda **_kwargs: {"status": "success"})
    monkeypatch.setattr("sys.argv", ["ytb_history", "generate-alerts"])

    code = cli.main()
    out = capsys.readouterr().out

    assert code == 0
    assert json.loads(out)["status"] == "success"


def test_cli_build_decision_layer_prints_json(monkeypatch, capsys) -> None:
    monkeypatch.setattr(
        cli,
        "build_decision_layer",
        lambda **kwargs: {"status": "success", "decision_dir": kwargs["data_dir"], "total_action_candidates": 2},
    )
    monkeypatch.setattr("sys.argv", ["ytb_history", "build-decision-layer", "--data-dir", "custom/data"])

    code = cli.main()
    out = capsys.readouterr().out

    assert code == 0
    parsed = json.loads(out)
    assert parsed["status"] == "success"
    assert parsed["decision_dir"] == "custom/data"


def test_cli_build_decision_layer_does_not_call_api_flows(monkeypatch, capsys) -> None:
    def _boom(**_kwargs):
        raise AssertionError("API flow should not be called")

    monkeypatch.setattr(cli, "run_pipeline", _boom)
    monkeypatch.setattr(cli, "run_dry_run", _boom)
    monkeypatch.setattr(cli, "build_decision_layer", lambda **_kwargs: {"status": "success"})
    monkeypatch.setattr("sys.argv", ["ytb_history", "build-decision-layer"])

    code = cli.main()
    out = capsys.readouterr().out

    assert code == 0
    assert json.loads(out)["status"] == "success"
