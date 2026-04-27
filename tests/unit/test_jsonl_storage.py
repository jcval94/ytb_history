from __future__ import annotations

import gzip

from ytb_history.storage.jsonl import read_jsonl_gz, write_jsonl_gz


def test_write_read_jsonl_gz_roundtrip(tmp_path) -> None:
    path = tmp_path / "history" / "snapshots.jsonl.gz"
    rows = [{"video_id": "v1", "title": "Título"}, {"video_id": "v2", "views": 10}]

    write_jsonl_gz(path, rows)

    assert read_jsonl_gz(path) == rows


def test_read_jsonl_gz_returns_empty_when_missing(tmp_path) -> None:
    assert read_jsonl_gz(tmp_path / "missing.jsonl.gz") == []


def test_write_jsonl_gz_creates_parent_dirs(tmp_path) -> None:
    path = tmp_path / "a" / "b" / "rows.jsonl.gz"

    write_jsonl_gz(path, [{"x": 1}])

    assert path.exists()


def test_read_jsonl_gz_ignores_empty_lines(tmp_path) -> None:
    path = tmp_path / "rows.jsonl.gz"
    path.parent.mkdir(parents=True, exist_ok=True)

    with gzip.open(path, "wt", encoding="utf-8") as handle:
        handle.write('{"a":1}\n\n  \n{"b":2}\n')

    assert read_jsonl_gz(path) == [{"a": 1}, {"b": 2}]
