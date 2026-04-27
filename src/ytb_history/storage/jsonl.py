"""JSONL read/write helpers."""

from __future__ import annotations

import gzip
import json
import os
import tempfile
from pathlib import Path
from typing import Any

from ytb_history.storage.atomic_write import atomic_write_text


def read_jsonl(path: str | Path) -> list[dict[str, Any]]:
    file_path = Path(path)
    if not file_path.exists():
        return []

    rows: list[dict[str, Any]] = []
    with file_path.open("r", encoding="utf-8") as handle:
        for line in handle:
            line = line.strip()
            if not line:
                continue
            rows.append(json.loads(line))
    return rows


def write_jsonl(path: str | Path, rows: list[dict[str, Any]]) -> None:
    file_path = Path(path)
    file_path.parent.mkdir(parents=True, exist_ok=True)

    payload = "\n".join(json.dumps(row, ensure_ascii=False) for row in rows)
    if payload:
        payload = f"{payload}\n"
    atomic_write_text(file_path, payload)


def read_jsonl_gz(path: str | Path) -> list[dict[str, Any]]:
    file_path = Path(path)
    if not file_path.exists():
        return []

    rows: list[dict[str, Any]] = []
    with gzip.open(file_path, "rt", encoding="utf-8") as handle:
        for line in handle:
            line = line.strip()
            if not line:
                continue
            rows.append(json.loads(line))
    return rows


def write_jsonl_gz(path: str | Path, rows: list[dict[str, Any]]) -> None:
    file_path = Path(path)
    file_path.parent.mkdir(parents=True, exist_ok=True)

    fd, tmp_name = tempfile.mkstemp(dir=str(file_path.parent), prefix=f".{file_path.name}.", suffix=".tmp")
    try:
        with os.fdopen(fd, "wb") as binary_handle:
            with gzip.GzipFile(fileobj=binary_handle, mode="wb") as gz_handle:
                for row in rows:
                    line = json.dumps(row, ensure_ascii=False).encode("utf-8")
                    gz_handle.write(line)
                    gz_handle.write(b"\n")
            binary_handle.flush()
            os.fsync(binary_handle.fileno())
        os.replace(tmp_name, file_path)
    finally:
        if os.path.exists(tmp_name):
            os.unlink(tmp_name)
