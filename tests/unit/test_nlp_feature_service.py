from __future__ import annotations

import csv
import json
from pathlib import Path

from ytb_history import cli
from ytb_history.services.nlp_feature_service import build_nlp_features


INPUT_FILES = [
    "latest_video_metrics.csv",
    "latest_title_metrics.csv",
    "latest_video_scores.csv",
    "latest_video_advanced_metrics.csv",
]


def _write_csv(path: Path, fieldnames: list[str], rows: list[dict[str, object]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def _prepare_data_dir(tmp_path: Path, *, few_rows: bool = False) -> Path:
    data_dir = tmp_path / "data"
    latest = data_dir / "analytics" / "latest"

    video_rows = [
        {
            "execution_date": "2026-04-28T00:00:00+00:00",
            "channel_id": "c1",
            "channel_name": "Canal IA",
            "video_id": "v1",
            "title": "ChatGPT y IA: guía 2026",
            "description": "Aprende inteligencia artificial y machine learning rápido",
        },
        {
            "execution_date": "2026-04-28T00:00:00+00:00",
            "channel_id": "c2",
            "channel_name": "Canal Finanzas",
            "video_id": "v2",
            "title": "Ahorra dinero ahora: inversión fácil",
            "description": "Finanzas personales, crédito y rendimiento",
        },
        {
            "execution_date": "2026-04-28T00:00:00+00:00",
            "channel_id": "c3",
            "channel_name": "Canal Noticias",
            "video_id": "v3",
            "title": "Última hora: nuevo lanzamiento",
            "description": "Noticia y actualización del mercado",
        },
        {
            "execution_date": "2026-04-28T00:00:00+00:00",
            "channel_id": "c4",
            "channel_name": "Canal Tips",
            "video_id": "v4",
            "title": "No hagas este error: por qué nadie te lo dice",
            "description": "Cuidado, evita el peor hábito",
        },
    ]
    if few_rows:
        video_rows = video_rows[:2]

    _write_csv(
        latest / "latest_video_metrics.csv",
        ["execution_date", "channel_id", "channel_name", "video_id", "title", "description"],
        video_rows,
    )
    _write_csv(
        latest / "latest_title_metrics.csv",
        ["video_id", "title"],
        [{"video_id": row["video_id"], "title": row["title"]} for row in video_rows],
    )
    _write_csv(
        latest / "latest_video_scores.csv",
        ["video_id", "title"],
        [{"video_id": row["video_id"], "title": row["title"]} for row in video_rows],
    )
    _write_csv(
        latest / "latest_video_advanced_metrics.csv",
        ["video_id", "title"],
        [{"video_id": row["video_id"], "title": row["title"]} for row in video_rows],
    )
    return data_dir


def test_build_nlp_features_generates_output_files(tmp_path: Path) -> None:
    data_dir = _prepare_data_dir(tmp_path)
    result = build_nlp_features(data_dir=data_dir)

    assert result["status"] in {"success", "success_with_warnings"}
    assert (data_dir / "nlp_features" / "latest_video_nlp_features.csv").exists()
    assert (data_dir / "nlp_features" / "latest_title_nlp_features.csv").exists()
    assert (data_dir / "nlp_features" / "latest_semantic_vectors.csv").exists()
    assert (data_dir / "nlp_features" / "latest_semantic_clusters.csv").exists()
    assert (data_dir / "nlp_features" / "nlp_feature_summary.json").exists()


def test_detects_ai_and_finance_semantics(tmp_path: Path) -> None:
    data_dir = _prepare_data_dir(tmp_path)
    build_nlp_features(data_dir=data_dir)

    rows = list(csv.DictReader((data_dir / "nlp_features" / "latest_video_nlp_features.csv").open("r", encoding="utf-8", newline="")))
    by_id = {row["video_id"]: row for row in rows}
    assert float(by_id["v1"]["ai_semantic_score"]) > 0
    assert float(by_id["v2"]["finance_semantic_score"]) > 0


def test_generates_hook_semantic_type(tmp_path: Path) -> None:
    data_dir = _prepare_data_dir(tmp_path)
    build_nlp_features(data_dir=data_dir)

    rows = list(csv.DictReader((data_dir / "nlp_features" / "latest_title_nlp_features.csv").open("r", encoding="utf-8", newline="")))
    by_id = {row["video_id"]: row for row in rows}
    assert by_id["v4"]["hook_semantic_type"] in {"warning", "curiosity"}
    assert by_id["v1"]["hook_semantic_type"] in {"tutorial", "ai", "promise"}


def test_lsa_and_clustering_fallback_with_few_rows(tmp_path: Path) -> None:
    data_dir = _prepare_data_dir(tmp_path, few_rows=True)
    summary = build_nlp_features(data_dir=data_dir)

    vectors = list(csv.DictReader((data_dir / "nlp_features" / "latest_semantic_vectors.csv").open("r", encoding="utf-8", newline="")))
    clusters = list(csv.DictReader((data_dir / "nlp_features" / "latest_semantic_clusters.csv").open("r", encoding="utf-8", newline="")))

    assert vectors
    assert all(set(row.keys()) == {"video_id"} for row in vectors)
    assert clusters
    assert all(row["semantic_cluster_id"] == "0" for row in clusters)
    assert any("LSA skipped" in warning for warning in summary["warnings"])


def test_no_api_no_search_list_and_writes_only_nlp_features(tmp_path: Path) -> None:
    data_dir = _prepare_data_dir(tmp_path)
    existing = {path.relative_to(data_dir) for path in data_dir.rglob("*") if path.is_file()}

    build_nlp_features(data_dir=data_dir)

    source = Path("src/ytb_history/services/nlp_feature_service.py").read_text(encoding="utf-8")
    assert "youtube_client" not in source
    assert "search.list" not in source

    updated = {path.relative_to(data_dir) for path in data_dir.rglob("*") if path.is_file()}
    created = updated - existing
    assert created
    assert all(str(path).startswith("nlp_features/") for path in created)


def test_cli_build_nlp_features_prints_json(monkeypatch, capsys, tmp_path: Path) -> None:
    data_dir = _prepare_data_dir(tmp_path)
    monkeypatch.setattr("sys.argv", ["ytb_history", "build-nlp-features", "--data-dir", str(data_dir)])

    code = cli.main()
    out = capsys.readouterr().out

    assert code == 0
    parsed = json.loads(out)
    assert parsed["nlp_mode"] == "tfidf_lsa_dictionary_v1"
    assert parsed["total_videos"] >= 1


def test_summary_json_structure(tmp_path: Path) -> None:
    data_dir = _prepare_data_dir(tmp_path)
    build_nlp_features(data_dir=data_dir)

    summary = json.loads((data_dir / "nlp_features" / "nlp_feature_summary.json").read_text(encoding="utf-8"))
    assert summary["nlp_mode"] == "tfidf_lsa_dictionary_v1"
    assert isinstance(summary["clusters"], int)
    assert isinstance(summary["warnings"], list)
    assert isinstance(summary["top_cluster_labels"], list)


def test_missing_files_do_not_fail(tmp_path: Path) -> None:
    data_dir = tmp_path / "data"
    latest = data_dir / "analytics" / "latest"
    latest.mkdir(parents=True, exist_ok=True)
    _write_csv(
        latest / "latest_video_metrics.csv",
        ["execution_date", "channel_id", "channel_name", "video_id", "title", "description"],
        [
            {
                "execution_date": "2026-04-28T00:00:00+00:00",
                "channel_id": "c1",
                "channel_name": "Canal",
                "video_id": "v1",
                "title": "hola",
                "description": "",
            }
        ],
    )

    result = build_nlp_features(data_dir=data_dir)
    assert result["status"] == "success_with_warnings"
    assert result["warnings"]

    for name in [
        "latest_video_nlp_features.csv",
        "latest_title_nlp_features.csv",
        "latest_semantic_vectors.csv",
        "latest_semantic_clusters.csv",
        "nlp_feature_summary.json",
    ]:
        assert (data_dir / "nlp_features" / name).exists()
