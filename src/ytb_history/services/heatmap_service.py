"""Public YouTube heatmap extraction for already-transcribed videos."""

from __future__ import annotations

import csv
import json
from datetime import datetime, timedelta, timezone
from hashlib import sha256
from pathlib import Path
from typing import Any

from ytb_history.services.transcript_store_service import list_transcribed_video_ids
from ytb_history.storage.jsonl import read_jsonl, write_jsonl, write_jsonl_gz
from ytb_history.utils.video_ids import is_transcribable_video_id_candidate

HEATMAPS_DIRNAME = "heatmaps"
REGISTRY_FILENAME = "heatmap_registry.jsonl"
SEGMENTS_FILENAME = "heatmap_segments.jsonl"
METADATA_FILENAME = "heatmap_metadata.json"

BUCKET_DAYS = {
    "1w": 7,
    "2w": 14,
    "4w": 28,
    "8w": 56,
}
BUCKET_ORDER = ["1w", "2w", "4w", "8w"]
VALID_BUCKETS = set(BUCKET_ORDER) | {"all"}


def _now_utc() -> datetime:
    return datetime.now(timezone.utc)


def _now_iso() -> str:
    return _now_utc().isoformat()


def _safe_video_id(video_id: str) -> str:
    safe = video_id.strip()
    if (
        not safe
        or "/" in safe
        or "\\" in safe
        or ".." in safe
        or not is_transcribable_video_id_candidate(safe)
    ):
        raise ValueError("video_id invalido para heatmap storage")
    return safe


def _heatmaps_root(data_dir: str | Path) -> Path:
    return Path(data_dir) / HEATMAPS_DIRNAME


def _video_dir(data_dir: str | Path, video_id: str) -> Path:
    return _heatmaps_root(data_dir) / "videos" / _safe_video_id(video_id)


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


def _read_json(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return {}
    return payload if isinstance(payload, dict) else {}


def _read_csv(path: Path) -> list[dict[str, str]]:
    if not path.exists():
        return []
    with path.open("r", encoding="utf-8", newline="") as handle:
        return list(csv.DictReader(handle))


def _parse_dt(value: Any) -> datetime | None:
    if not value:
        return None
    text = str(value).strip()
    if not text:
        return None
    if text.endswith("Z"):
        text = f"{text[:-1]}+00:00"
    try:
        parsed = datetime.fromisoformat(text)
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _to_int(value: Any) -> int | None:
    if value in (None, ""):
        return None
    try:
        return int(float(str(value)))
    except (TypeError, ValueError):
        return None


def _bucket_for_age(upload_date: datetime, now: datetime) -> str | None:
    age_days = (now - upload_date).total_seconds() / 86400
    if age_days < BUCKET_DAYS["1w"]:
        return None
    if age_days >= BUCKET_DAYS["8w"]:
        return "8w"
    if age_days >= BUCKET_DAYS["4w"]:
        return "4w"
    if age_days >= BUCKET_DAYS["2w"]:
        return "2w"
    return "1w"


def _next_retry_after(upload_date: datetime, current_bucket: str) -> str | None:
    if current_bucket == "1w":
        return (upload_date + timedelta(days=BUCKET_DAYS["2w"])).isoformat()
    if current_bucket == "2w":
        return (upload_date + timedelta(days=BUCKET_DAYS["4w"])).isoformat()
    if current_bucket == "4w":
        return (upload_date + timedelta(days=BUCKET_DAYS["8w"])).isoformat()
    return None


def _load_heatmap_registry(data_dir: str | Path) -> list[dict[str, Any]]:
    return read_jsonl(_heatmaps_root(data_dir) / REGISTRY_FILENAME)


def _save_heatmap_registry(data_dir: str | Path, rows: list[dict[str, Any]]) -> None:
    deduped: dict[str, dict[str, Any]] = {}
    for row in rows:
        video_id = str(row.get("video_id", "")).strip()
        if video_id:
            deduped[video_id] = row
    ordered = [deduped[key] for key in sorted(deduped)]
    write_jsonl(_heatmaps_root(data_dir) / REGISTRY_FILENAME, ordered)


def _load_latest_video_metadata(data_dir: str | Path) -> dict[str, dict[str, Any]]:
    root = Path(data_dir)
    rows: list[dict[str, Any]] = []
    source_paths = [
        root / "analytics" / "latest" / "latest_video_metrics.csv",
        root / "analytics" / "formats" / "videos" / "latest" / "latest_video_metrics.csv",
        root / "exports" / "latest" / "latest_snapshots.csv",
        root / "exports" / "latest" / "video_growth_summary.csv",
    ]
    for path in source_paths:
        rows.extend(_read_csv(path))

    for path in sorted(root.glob("exports/dt=*/run=*/latest_snapshots.csv")):
        rows.extend(_read_csv(path))
    for path in sorted(root.glob("exports/dt=*/run=*/video_growth_summary.csv")):
        rows.extend(_read_csv(path))

    for row in read_jsonl(root / "state" / "tracked_videos_catalog.jsonl"):
        rows.append(row)

    by_video_id: dict[str, dict[str, Any]] = {}
    for row in rows:
        video_id = str(row.get("video_id", "")).strip()
        if not video_id or not is_transcribable_video_id_candidate(video_id):
            continue
        current = by_video_id.setdefault(video_id, {"video_id": video_id})
        for key in ("channel_id", "channel_name", "title", "upload_date", "duration_seconds"):
            value = row.get(key)
            if value not in (None, ""):
                current[key] = value
        if "first_seen_date" in row and "upload_date" not in current:
            current["upload_date"] = row.get("first_seen_date")
    return by_video_id


def _load_transcript_metadata(data_dir: str | Path, video_id: str) -> dict[str, Any]:
    path = Path(data_dir) / "transcripts" / "videos" / _safe_video_id(video_id) / "transcript_metadata.json"
    return _read_json(path)


def _merge_transcript_metadata(data_dir: str | Path, video_id: str, updates: dict[str, Any]) -> None:
    safe_video_id = _safe_video_id(video_id)
    path = Path(data_dir) / "transcripts" / "videos" / safe_video_id / "transcript_metadata.json"
    if not path.exists():
        return
    payload = _read_json(path)
    payload.update(updates)
    _write_json(path, payload)


def _normalise_heatmap_segments(raw_heatmap: Any, *, duration_seconds: int | None = None) -> list[dict[str, Any]]:
    if not isinstance(raw_heatmap, list) or not raw_heatmap:
        return []
    segments: list[dict[str, Any]] = []
    for index, item in enumerate(raw_heatmap):
        if not isinstance(item, dict):
            return []
        raw_start = item.get("start_time") if "start_time" in item else item.get("start_seconds")
        raw_end = item.get("end_time") if "end_time" in item else item.get("end_seconds")
        raw_value = item.get("value")
        if raw_value is None:
            raw_value = item.get("heatMarkerIntensityScoreNormalized")
        try:
            start_seconds = float(raw_start)
            value = float(raw_value)
            end_seconds = float(raw_end) if raw_end is not None else None
        except (TypeError, ValueError):
            return []
        if start_seconds < 0 or value < 0:
            return []
        if end_seconds is None:
            if index + 1 < len(raw_heatmap):
                next_item = raw_heatmap[index + 1]
                if not isinstance(next_item, dict):
                    return []
                next_start = next_item.get("start_time") if "start_time" in next_item else next_item.get("start_seconds")
                try:
                    end_seconds = float(next_start)
                except (TypeError, ValueError):
                    return []
            elif duration_seconds is not None:
                end_seconds = float(duration_seconds)
            else:
                end_seconds = start_seconds
        if end_seconds < start_seconds:
            return []
        segments.append(
            {
                "segment_index": index,
                "start_seconds": round(start_seconds, 6),
                "end_seconds": round(end_seconds, 6),
                "value": round(value, 6),
            }
        )
    return segments


def _segments_hash(segments: list[dict[str, Any]]) -> str:
    payload = json.dumps(segments, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    return sha256(payload.encode("utf-8")).hexdigest()


def _extract_public_heatmap(video_id: str, *, duration_seconds: int | None = None) -> tuple[str, list[dict[str, Any]], str | None]:
    safe_video_id = _safe_video_id(video_id)
    try:
        import yt_dlp  # type: ignore[import-not-found]
    except ImportError:
        return "extractor_error", [], "yt_dlp_not_installed"

    url = f"https://www.youtube.com/watch?v={safe_video_id}"
    try:
        with yt_dlp.YoutubeDL({"skip_download": True, "quiet": True, "no_warnings": True}) as ydl:
            info = ydl.extract_info(url, download=False)
    except Exception as exc:  # noqa: BLE001 - yt-dlp raises extractor-specific exceptions.
        message = str(exc)
        lower = message.lower()
        if any(token in lower for token in ("unavailable", "private video", "removed", "not available")):
            return "video_unavailable", [], message[:500]
        return "extractor_error", [], message[:500]

    if not isinstance(info, dict):
        return "extractor_error", [], "yt_dlp_returned_non_dict_info"
    raw_heatmap = info.get("heatmap")
    if raw_heatmap in (None, []):
        return "not_available_yet", [], None
    segments = _normalise_heatmap_segments(raw_heatmap, duration_seconds=duration_seconds)
    if not segments:
        return "invalid_heatmap", [], "invalid_or_unexpected_heatmap_shape"
    return "success", segments, None


def _success_registry_row(
    *,
    candidate: dict[str, Any],
    segments_path: str,
    metadata_path: str,
    heatmap_sha256: str,
    extracted_at: str,
    attempts: int,
) -> dict[str, Any]:
    return {
        "video_id": candidate["video_id"],
        "channel_id": candidate.get("channel_id", ""),
        "channel_name": candidate.get("channel_name", ""),
        "title": candidate.get("title", ""),
        "upload_date": candidate.get("upload_date", ""),
        "duration_seconds": candidate.get("duration_seconds"),
        "current_bucket": candidate["current_bucket"],
        "attempt_count": attempts,
        "last_attempt_at": extracted_at,
        "last_status": "success",
        "next_retry_after": None,
        "heatmap_available": True,
        "heatmap_sha256": heatmap_sha256,
        "segments_path": segments_path,
        "metadata_path": metadata_path,
        "error_category": None,
        "error_message": None,
    }


def _failure_registry_row(
    *,
    candidate: dict[str, Any],
    status: str,
    error_message: str | None,
    attempted_at: str,
    attempts: int,
) -> dict[str, Any]:
    upload_date = candidate["upload_dt"]
    current_bucket = candidate["current_bucket"]
    exhausted = current_bucket == "8w" and status == "not_available_yet"
    return {
        "video_id": candidate["video_id"],
        "channel_id": candidate.get("channel_id", ""),
        "channel_name": candidate.get("channel_name", ""),
        "title": candidate.get("title", ""),
        "upload_date": candidate.get("upload_date", ""),
        "duration_seconds": candidate.get("duration_seconds"),
        "current_bucket": current_bucket,
        "attempt_count": attempts,
        "last_attempt_at": attempted_at,
        "last_status": "exhausted_after_8w" if exhausted else status,
        "next_retry_after": None if exhausted else _next_retry_after(upload_date, current_bucket),
        "heatmap_available": False,
        "heatmap_sha256": None,
        "segments_path": None,
        "metadata_path": None,
        "error_category": "not_available" if exhausted else status,
        "error_message": error_message,
    }


def _build_candidates(
    *,
    data_dir: str | Path,
    bucket: str,
    force: bool,
    now: datetime,
) -> tuple[list[dict[str, Any]], dict[str, int], list[str]]:
    if bucket not in VALID_BUCKETS:
        raise ValueError(f"bucket must be one of: {', '.join(sorted(VALID_BUCKETS))}")

    transcribed_ids = list_transcribed_video_ids(data_dir=data_dir)
    metadata_by_video = _load_latest_video_metadata(data_dir)
    registry_by_video = {str(row.get("video_id", "")).strip(): row for row in _load_heatmap_registry(data_dir)}
    metadata_video_ids = {video_id for video_id in metadata_by_video if is_transcribable_video_id_candidate(video_id)}
    counters = {
        "transcribed_videos": len(transcribed_ids),
        "skipped_not_transcribed": len(metadata_video_ids - transcribed_ids),
        "skipped_missing_metadata": 0,
        "skipped_missing_upload_date": 0,
        "skipped_too_young": 0,
        "skipped_bucket_filter": 0,
        "skipped_already_success": 0,
        "skipped_waiting_next_bucket": 0,
        "skipped_exhausted_after_8w": 0,
    }
    warnings: list[str] = []
    candidates: list[dict[str, Any]] = []

    for video_id in sorted(transcribed_ids):
        try:
            safe_video_id = _safe_video_id(video_id)
        except ValueError:
            continue
        item = dict(metadata_by_video.get(safe_video_id, {}))
        transcript_metadata = _load_transcript_metadata(data_dir, safe_video_id)
        for key in ("channel_id", "channel_name", "title", "duration_seconds"):
            if item.get(key) in (None, "") and transcript_metadata.get(key) not in (None, ""):
                item[key] = transcript_metadata.get(key)
        if item.get("upload_date") in (None, "") and transcript_metadata.get("upload_date") not in (None, ""):
            item["upload_date"] = transcript_metadata.get("upload_date")
        item["video_id"] = safe_video_id

        if len(item) <= 1:
            counters["skipped_missing_metadata"] += 1
        upload_dt = _parse_dt(item.get("upload_date"))
        if upload_dt is None:
            counters["skipped_missing_upload_date"] += 1
            continue
        current_bucket = _bucket_for_age(upload_dt, now)
        if current_bucket is None:
            counters["skipped_too_young"] += 1
            continue
        if bucket != "all" and bucket != current_bucket:
            counters["skipped_bucket_filter"] += 1
            continue

        existing = registry_by_video.get(safe_video_id, {})
        existing_status = str(existing.get("last_status", "")).strip()
        if existing_status == "success" and not force:
            counters["skipped_already_success"] += 1
            continue
        if existing_status == "exhausted_after_8w" and not force:
            counters["skipped_exhausted_after_8w"] += 1
            continue
        next_retry_at = _parse_dt(existing.get("next_retry_after"))
        if next_retry_at and next_retry_at > now and not force:
            counters["skipped_waiting_next_bucket"] += 1
            continue
        if str(existing.get("current_bucket", "")).strip() == current_bucket and existing_status and not force:
            counters["skipped_waiting_next_bucket"] += 1
            continue

        item["upload_dt"] = upload_dt
        item["current_bucket"] = current_bucket
        item["previous_attempt_count"] = int(existing.get("attempt_count", 0) or 0)
        candidates.append(item)

    candidates.sort(
        key=lambda row: (
            BUCKET_ORDER.index(str(row["current_bucket"])),
            str(row.get("upload_date", "")),
            str(row["video_id"]),
        ),
        reverse=True,
    )
    if counters["skipped_missing_metadata"]:
        warnings.append("transcribed_videos_missing_video_metadata")
    if counters["skipped_missing_upload_date"]:
        warnings.append("transcribed_videos_missing_upload_date")
    return candidates, counters, warnings


def _write_success_artifacts(
    *,
    data_dir: str | Path,
    candidate: dict[str, Any],
    segments: list[dict[str, Any]],
    extracted_at: str,
    heatmap_sha256: str,
) -> tuple[str, str]:
    video_id = candidate["video_id"]
    video_dir = _video_dir(data_dir, video_id)
    segments_path = video_dir / SEGMENTS_FILENAME
    metadata_path = video_dir / METADATA_FILENAME
    existing_metadata = _read_json(metadata_path)
    if existing_metadata.get("heatmap_sha256") != heatmap_sha256 or not segments_path.exists():
        rows = [{"video_id": video_id, **segment} for segment in segments]
        write_jsonl(segments_path, rows)
    metadata = {
        "schema_version": "heatmap_metadata_v1",
        "video_id": video_id,
        "channel_id": candidate.get("channel_id", ""),
        "channel_name": candidate.get("channel_name", ""),
        "title": candidate.get("title", ""),
        "upload_date": candidate.get("upload_date", ""),
        "duration_seconds": candidate.get("duration_seconds"),
        "bucket": candidate["current_bucket"],
        "extracted_at": extracted_at,
        "source": "yt_dlp_public_metadata",
        "segment_count": len(segments),
        "heatmap_sha256": heatmap_sha256,
        "segments_path": str(segments_path),
    }
    _write_json(metadata_path, metadata)
    _merge_transcript_metadata(
        data_dir,
        video_id,
        {
            "heatmap_available": True,
            "heatmap_segments_path": str(segments_path),
            "heatmap_sha256": heatmap_sha256,
            "heatmap_extracted_at": extracted_at,
        },
    )
    return str(segments_path), str(metadata_path)


def _unique_run_dir(root: Path, generated_at: datetime) -> Path:
    base = root / "runs" / f"dt={generated_at.strftime('%Y-%m-%d')}" / f"run={generated_at.strftime('%H%M%SZ')}"
    if not base.exists():
        return base
    for index in range(1, 1000):
        candidate = root / "runs" / f"dt={generated_at.strftime('%Y-%m-%d')}" / f"run={generated_at.strftime('%H%M%SZ')}-{index:03d}"
        if not candidate.exists():
            return candidate
    raise RuntimeError(f"Could not allocate unique heatmap run directory under {root}")


def extract_heatmaps(
    *,
    data_dir: str | Path = "data",
    limit: int = 50,
    bucket: str = "all",
    force: bool = False,
    dry_run: bool = False,
) -> dict[str, Any]:
    """Extract public YouTube heatmaps for transcribed videos."""

    now = _now_utc()
    generated_at = now.isoformat()
    root = _heatmaps_root(data_dir)
    candidates, counters, warnings = _build_candidates(
        data_dir=data_dir,
        bucket=bucket,
        force=force,
        now=now,
    )
    selected = candidates[: max(0, int(limit))]
    if len(candidates) > len(selected):
        warnings.append("candidate_limit_applied")

    attempts: list[dict[str, Any]] = []
    status_counts: dict[str, int] = {}
    registry_by_video = {str(row.get("video_id", "")).strip(): row for row in _load_heatmap_registry(data_dir)}

    if not dry_run:
        root.mkdir(parents=True, exist_ok=True)

    for candidate in selected:
        video_id = candidate["video_id"]
        attempt_at = _now_iso()
        if dry_run:
            status = "dry_run"
            segments: list[dict[str, Any]] = []
            error_message = None
        else:
            status, segments, error_message = _extract_public_heatmap(
                video_id,
                duration_seconds=_to_int(candidate.get("duration_seconds")),
            )
        attempts_count = int(candidate.get("previous_attempt_count", 0) or 0) + (0 if dry_run else 1)
        heatmap_sha = _segments_hash(segments) if segments else None
        attempt_row: dict[str, Any] = {
            "attempted_at": attempt_at,
            "video_id": video_id,
            "channel_id": candidate.get("channel_id", ""),
            "channel_name": candidate.get("channel_name", ""),
            "title": candidate.get("title", ""),
            "upload_date": candidate.get("upload_date", ""),
            "duration_seconds": candidate.get("duration_seconds"),
            "bucket": candidate["current_bucket"],
            "status": status,
            "segment_count": len(segments),
            "heatmap_sha256": heatmap_sha,
            "error_message": error_message,
        }
        attempts.append(attempt_row)
        status_counts[status] = status_counts.get(status, 0) + 1

        if dry_run:
            continue

        if status == "success" and heatmap_sha is not None:
            segments_path, metadata_path = _write_success_artifacts(
                data_dir=data_dir,
                candidate=candidate,
                segments=segments,
                extracted_at=attempt_at,
                heatmap_sha256=heatmap_sha,
            )
            registry_by_video[video_id] = _success_registry_row(
                candidate=candidate,
                segments_path=segments_path,
                metadata_path=metadata_path,
                heatmap_sha256=heatmap_sha,
                extracted_at=attempt_at,
                attempts=attempts_count,
            )
            attempt_row["segments_path"] = segments_path
            attempt_row["metadata_path"] = metadata_path
        else:
            registry_by_video[video_id] = _failure_registry_row(
                candidate=candidate,
                status=status,
                error_message=error_message,
                attempted_at=attempt_at,
                attempts=attempts_count,
            )

    run_dir = _unique_run_dir(root, now)
    report = {
        "status": "success",
        "schema_version": "heatmap_run_report_v1",
        "generated_at": generated_at,
        "mode": "dry_run" if dry_run else "extract",
        "data_dir": str(data_dir),
        "bucket": bucket,
        "force": force,
        "limit": limit,
        "eligible_candidates": len(candidates),
        "attempted_count": len(selected),
        "status_counts": status_counts,
        "selection_counters": counters,
        "run_dir": str(run_dir),
        "registry_path": str(root / REGISTRY_FILENAME),
        "warnings": warnings,
        "errors": [],
    }

    if not dry_run:
        _save_heatmap_registry(data_dir, list(registry_by_video.values()))
        _write_json(run_dir / "heatmap_run_report.json", report)
        write_jsonl_gz(run_dir / "heatmap_attempts.jsonl.gz", attempts)
        _write_json(root / "latest_heatmap_run_report.json", report)

    return report
