"""Analytics data mart builders for latest exported run."""

from __future__ import annotations

import csv
import json
import re
from datetime import datetime, timezone
from pathlib import Path
from statistics import median
from typing import Any

VIDEO_METRICS_COLUMNS = [
    "execution_date",
    "channel_id",
    "channel_name",
    "video_id",
    "title",
    "upload_date",
    "duration_seconds",
    "duration_bucket",
    "is_short",
    "views",
    "likes",
    "comments",
    "views_delta",
    "likes_delta",
    "comments_delta",
    "engagement_rate",
    "like_rate",
    "comment_rate",
    "delta_engagement_rate",
    "video_age_days",
    "video_age_hours",
    "views_per_day_since_upload",
    "is_new_video",
    "title_changed",
    "description_changed",
    "tags_changed",
    "metadata_changed",
    "thumbnail_url_present",
    "growth_rank",
    "engagement_rank",
]

CHANNEL_METRICS_COLUMNS = [
    "execution_date",
    "channel_id",
    "channel_name",
    "videos_tracked",
    "new_videos",
    "total_views",
    "total_views_delta",
    "total_likes_delta",
    "total_comments_delta",
    "avg_engagement_rate",
    "median_views_delta",
    "top_video_id",
    "top_video_title",
    "top_video_views_delta",
    "shorts_count",
    "mid_count",
    "long_count",
]

TITLE_METRICS_COLUMNS = [
    "execution_date",
    "channel_id",
    "channel_name",
    "video_id",
    "title",
    "title_length_chars",
    "title_word_count",
    "has_number",
    "has_question",
    "has_colon",
    "has_parentheses",
    "has_year",
    "has_currency_symbol",
    "has_negative_word",
    "has_urgency_word",
    "has_promise_word",
    "has_ai_word",
    "has_finance_word",
    "title_changed",
    "views_delta",
    "engagement_rate",
]

NEGATIVE_WORDS = {"error", "errores", "nunca", "peor", "fracaso", "peligro", "perder"}
URGENCY_WORDS = {"hoy", "ahora", "nuevo", "urgente", "último", "ya"}
PROMISE_WORDS = {"gana", "ganar", "ahorra", "ahorrar", "mejora", "aprende", "fácil"}
AI_WORDS = {"ia", "ai", "chatgpt", "gemini", "claude", "inteligencia artificial"}
FINANCE_WORDS = {"dinero", "inversión", "invertir", "finanzas", "ahorro", "deuda", "crédito", "banco"}

FUTURE_FEATURE_INPUTS = [
    "features/thumbnails/latest_thumbnail_features.csv",
    "features/transcripts/latest_transcript_features.csv",
    "features/topics/latest_topic_features.csv",
]


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _to_int(value: Any) -> int | None:
    if value in (None, ""):
        return None
    try:
        return int(str(value))
    except (TypeError, ValueError):
        return None


def _to_bool(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    if value is None:
        return False
    return str(value).strip().lower() in {"1", "true", "yes", "y"}


def _to_float_string(value: float | None) -> str:
    if value is None:
        return ""
    return str(value)


def _parse_iso8601(value: str | None) -> datetime | None:
    if not value:
        return None
    raw = value.strip()
    if not raw:
        return None
    try:
        if raw.endswith("Z"):
            return datetime.fromisoformat(raw.replace("Z", "+00:00"))
        return datetime.fromisoformat(raw)
    except ValueError:
        return None


def _read_csv(path: Path) -> list[dict[str, str]]:
    with path.open("r", encoding="utf-8", newline="") as handle:
        return list(csv.DictReader(handle))


def _write_csv(path: Path, columns: list[str], rows: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=columns)
        writer.writeheader()
        for row in rows:
            writer.writerow({k: row.get(k, "") for k in columns})


def _latest_export_dir(exports_root: Path) -> Path | None:
    latest_alias = exports_root / "latest"
    if latest_alias.is_dir():
        return latest_alias

    candidates: list[Path] = []
    for dt_dir in exports_root.glob("dt=*"):
        if not dt_dir.is_dir():
            continue
        for run_dir in dt_dir.glob("run=*"):
            if run_dir.is_dir():
                candidates.append(run_dir)

    if not candidates:
        return None
    return max(candidates, key=lambda p: (p.parent.name, p.name))


def _duration_bucket(duration_seconds: int | None) -> str:
    if duration_seconds is None:
        return "unknown"
    if duration_seconds <= 60:
        return "short"
    if duration_seconds <= 600:
        return "mid"
    return "long"


def _rank_desc(values: list[float | None]) -> list[str]:
    indexed_valid = [(idx, value) for idx, value in enumerate(values) if value is not None]
    indexed_valid.sort(key=lambda item: item[1], reverse=True)
    ranks = [""] * len(values)
    for position, (idx, _value) in enumerate(indexed_valid, start=1):
        ranks[idx] = str(position)
    return ranks


def _contains_word_or_phrase(text: str, words: set[str]) -> bool:
    normalized = text.lower()
    tokens = re.findall(r"[\wáéíóúñü]+", normalized)
    token_set = set(tokens)
    for word in words:
        if " " in word:
            if word in normalized:
                return True
        elif word in token_set:
            return True
    return False


def _safe_rel(path: Path, base: Path) -> str:
    try:
        return str(path.resolve().relative_to(base.resolve()))
    except ValueError:
        return str(path)


def build_analytics(*, data_dir: str | Path = "data") -> dict[str, Any]:
    data_root = Path(data_dir)
    exports_root = data_root / "exports"
    analytics_root = data_root / "analytics"
    analytics_latest = analytics_root / "latest"

    result: dict[str, Any] = {
        "status": "failed",
        "source_export_dir": None,
        "analytics_dir": str(analytics_latest),
        "warnings": [],
        "outputs": {},
    }
    warnings: list[str] = result["warnings"]

    if not exports_root.exists():
        warnings.append(f"No existe data/exports: {exports_root}")
        return result

    source_export_dir = _latest_export_dir(exports_root)
    if source_export_dir is None:
        warnings.append(f"No hay corridas exportadas dentro de: {exports_root}")
        return result

    required_files = {
        "video_growth_summary": source_export_dir / "video_growth_summary.csv",
        "latest_snapshots": source_export_dir / "latest_snapshots.csv",
        "latest_deltas": source_export_dir / "latest_deltas.csv",
        "export_summary": source_export_dir / "export_summary.json",
    }
    missing = [str(path) for path in required_files.values() if not path.exists()]
    if missing:
        warnings.append(f"Faltan archivos de export requeridos: {missing}")
        return result

    growth_rows = _read_csv(required_files["video_growth_summary"])
    snapshot_rows = _read_csv(required_files["latest_snapshots"])
    delta_rows = _read_csv(required_files["latest_deltas"])
    export_summary = json.loads(required_files["export_summary"].read_text(encoding="utf-8"))

    snapshots_by_video = {str(row.get("video_id", "")): row for row in snapshot_rows}
    deltas_by_video = {str(row.get("video_id", "")): row for row in delta_rows}

    video_rows: list[dict[str, Any]] = []
    for growth in growth_rows:
        video_id = str(growth.get("video_id", ""))
        snapshot = snapshots_by_video.get(video_id, {})
        delta = deltas_by_video.get(video_id, {})

        execution_date = str(growth.get("execution_date", "") or snapshot.get("execution_date", ""))
        upload_date = str(growth.get("upload_date", "") or snapshot.get("upload_date", ""))

        duration_seconds = _to_int(growth.get("duration_seconds") or snapshot.get("duration_seconds"))
        views = _to_int(growth.get("views") or snapshot.get("views"))
        likes = _to_int(growth.get("likes") or snapshot.get("likes"))
        comments = _to_int(growth.get("comments") or snapshot.get("comments"))
        views_delta = _to_int(growth.get("views_delta") or delta.get("views_delta"))
        likes_delta = _to_int(growth.get("likes_delta") or delta.get("likes_delta"))
        comments_delta = _to_int(growth.get("comments_delta") or delta.get("comments_delta"))

        engagement_rate = None
        like_rate = None
        comment_rate = None
        if views is not None and views > 0:
            engagement_rate = ((likes or 0) + (comments or 0)) / views
            like_rate = (likes or 0) / views
            comment_rate = (comments or 0) / views

        delta_engagement_rate = None
        if views_delta is not None and views_delta > 0:
            delta_engagement_rate = ((likes_delta or 0) + (comments_delta or 0)) / views_delta

        execution_dt = _parse_iso8601(execution_date)
        upload_dt = _parse_iso8601(upload_date)
        video_age_days = None
        video_age_hours = None
        if execution_dt is not None and upload_dt is not None:
            delta_time = execution_dt - upload_dt
            video_age_hours = delta_time.total_seconds() / 3600
            video_age_days = video_age_hours / 24

        views_per_day_since_upload = None
        if views is not None and video_age_days is not None:
            views_per_day_since_upload = views / max(video_age_days, 1 / 24)

        title_changed = _to_bool(growth.get("title_changed") or delta.get("title_changed"))
        description_changed = _to_bool(growth.get("description_changed") or delta.get("description_changed"))
        tags_changed = _to_bool(growth.get("tags_changed") or delta.get("tags_changed"))
        metadata_changed = title_changed or description_changed or tags_changed

        thumbnail_url = str(snapshot.get("thumbnail_url", "") or "")

        row = {
            "execution_date": execution_date,
            "channel_id": str(growth.get("channel_id", "") or snapshot.get("channel_id", "")),
            "channel_name": str(growth.get("channel_name", "") or snapshot.get("channel_name", "")),
            "video_id": video_id,
            "title": str(growth.get("title", "") or snapshot.get("title", "")),
            "upload_date": upload_date,
            "duration_seconds": "" if duration_seconds is None else str(duration_seconds),
            "duration_bucket": _duration_bucket(duration_seconds),
            "is_short": str(duration_seconds is not None and duration_seconds <= 60),
            "views": "" if views is None else str(views),
            "likes": "" if likes is None else str(likes),
            "comments": "" if comments is None else str(comments),
            "views_delta": "" if views_delta is None else str(views_delta),
            "likes_delta": "" if likes_delta is None else str(likes_delta),
            "comments_delta": "" if comments_delta is None else str(comments_delta),
            "engagement_rate": _to_float_string(engagement_rate),
            "like_rate": _to_float_string(like_rate),
            "comment_rate": _to_float_string(comment_rate),
            "delta_engagement_rate": _to_float_string(delta_engagement_rate),
            "video_age_days": _to_float_string(video_age_days),
            "video_age_hours": _to_float_string(video_age_hours),
            "views_per_day_since_upload": _to_float_string(views_per_day_since_upload),
            "is_new_video": str(_to_bool(growth.get("is_new_video") or delta.get("is_new_video"))),
            "title_changed": str(title_changed),
            "description_changed": str(description_changed),
            "tags_changed": str(tags_changed),
            "metadata_changed": str(metadata_changed),
            "thumbnail_url_present": str(bool(thumbnail_url.strip())),
            "growth_rank": "",
            "engagement_rank": "",
        }
        video_rows.append(row)

    growth_rank_values = [_to_int(row["views_delta"]) if row["views_delta"] != "" else None for row in video_rows]
    engagement_rank_values = [float(row["engagement_rate"]) if row["engagement_rate"] != "" else None for row in video_rows]
    growth_ranks = _rank_desc(growth_rank_values)
    engagement_ranks = _rank_desc(engagement_rank_values)
    for idx, row in enumerate(video_rows):
        row["growth_rank"] = growth_ranks[idx]
        row["engagement_rank"] = engagement_ranks[idx]

    channel_acc: dict[str, dict[str, Any]] = {}
    for row in video_rows:
        channel_id = row["channel_id"]
        channel = channel_acc.setdefault(
            channel_id,
            {
                "execution_date": row["execution_date"],
                "channel_id": channel_id,
                "channel_name": row["channel_name"],
                "videos_tracked": 0,
                "new_videos": 0,
                "total_views": 0,
                "total_views_delta": 0,
                "total_likes_delta": 0,
                "total_comments_delta": 0,
                "engagement_values": [],
                "views_delta_values": [],
                "top_video_id": "",
                "top_video_title": "",
                "top_video_views_delta": None,
                "shorts_count": 0,
                "mid_count": 0,
                "long_count": 0,
            },
        )
        channel["videos_tracked"] += 1
        if _to_bool(row["is_new_video"]):
            channel["new_videos"] += 1

        channel["total_views"] += _to_int(row["views"]) or 0
        channel["total_views_delta"] += _to_int(row["views_delta"]) or 0
        channel["total_likes_delta"] += _to_int(row["likes_delta"]) or 0
        channel["total_comments_delta"] += _to_int(row["comments_delta"]) or 0

        if row["engagement_rate"] != "":
            channel["engagement_values"].append(float(row["engagement_rate"]))
        if row["views_delta"] != "":
            val = int(row["views_delta"])
            channel["views_delta_values"].append(val)
            if channel["top_video_views_delta"] is None or val > channel["top_video_views_delta"]:
                channel["top_video_views_delta"] = val
                channel["top_video_id"] = row["video_id"]
                channel["top_video_title"] = row["title"]

        if row["duration_bucket"] == "short":
            channel["shorts_count"] += 1
        elif row["duration_bucket"] == "mid":
            channel["mid_count"] += 1
        elif row["duration_bucket"] == "long":
            channel["long_count"] += 1

    channel_rows: list[dict[str, Any]] = []
    for channel in channel_acc.values():
        avg_engagement_rate = (
            sum(channel["engagement_values"]) / len(channel["engagement_values"])
            if channel["engagement_values"]
            else None
        )
        median_views_delta = median(channel["views_delta_values"]) if channel["views_delta_values"] else None
        channel_rows.append(
            {
                "execution_date": channel["execution_date"],
                "channel_id": channel["channel_id"],
                "channel_name": channel["channel_name"],
                "videos_tracked": str(channel["videos_tracked"]),
                "new_videos": str(channel["new_videos"]),
                "total_views": str(channel["total_views"]),
                "total_views_delta": str(channel["total_views_delta"]),
                "total_likes_delta": str(channel["total_likes_delta"]),
                "total_comments_delta": str(channel["total_comments_delta"]),
                "avg_engagement_rate": _to_float_string(avg_engagement_rate),
                "median_views_delta": _to_float_string(median_views_delta),
                "top_video_id": channel["top_video_id"],
                "top_video_title": channel["top_video_title"],
                "top_video_views_delta": "" if channel["top_video_views_delta"] is None else str(channel["top_video_views_delta"]),
                "shorts_count": str(channel["shorts_count"]),
                "mid_count": str(channel["mid_count"]),
                "long_count": str(channel["long_count"]),
            }
        )

    title_rows: list[dict[str, Any]] = []
    for row in video_rows:
        title = row["title"]
        words = re.findall(r"[\wáéíóúñü]+", title.lower())
        title_rows.append(
            {
                "execution_date": row["execution_date"],
                "channel_id": row["channel_id"],
                "channel_name": row["channel_name"],
                "video_id": row["video_id"],
                "title": title,
                "title_length_chars": str(len(title)),
                "title_word_count": str(len(words)),
                "has_number": str(bool(re.search(r"\d", title))),
                "has_question": str("?" in title or "¿" in title),
                "has_colon": str(":" in title),
                "has_parentheses": str("(" in title or ")" in title),
                "has_year": str(bool(re.search(r"\b(?:19|20)\d{2}\b", title))),
                "has_currency_symbol": str(any(symbol in title for symbol in ["$", "€", "£", "¥"])),
                "has_negative_word": str(_contains_word_or_phrase(title, NEGATIVE_WORDS)),
                "has_urgency_word": str(_contains_word_or_phrase(title, URGENCY_WORDS)),
                "has_promise_word": str(_contains_word_or_phrase(title, PROMISE_WORDS)),
                "has_ai_word": str(_contains_word_or_phrase(title, AI_WORDS)),
                "has_finance_word": str(_contains_word_or_phrase(title, FINANCE_WORDS)),
                "title_changed": row["title_changed"],
                "views_delta": row["views_delta"],
                "engagement_rate": row["engagement_rate"],
            }
        )

    execution_date = ""
    if video_rows:
        execution_date = str(video_rows[0]["execution_date"])
    elif isinstance(export_summary, dict):
        execution_date = str(export_summary.get("execution_date", ""))

    run_metrics = {
        "execution_date": execution_date,
        "videos_total": len(video_rows),
        "channels_total": len(channel_rows),
        "total_views": sum((_to_int(row["views"]) or 0) for row in video_rows),
        "total_views_delta": sum((_to_int(row["views_delta"]) or 0) for row in video_rows),
        "total_likes_delta": sum((_to_int(row["likes_delta"]) or 0) for row in video_rows),
        "total_comments_delta": sum((_to_int(row["comments_delta"]) or 0) for row in video_rows),
        "avg_engagement_rate": (
            sum(float(row["engagement_rate"]) for row in video_rows if row["engagement_rate"] != "")
            / len([1 for row in video_rows if row["engagement_rate"] != ""])
            if any(row["engagement_rate"] != "" for row in video_rows)
            else None
        ),
        "generated_at": _now_iso(),
        "source_export_dir": str(source_export_dir),
        "warnings": warnings,
    }

    future_found = [rel for rel in FUTURE_FEATURE_INPUTS if (data_root / rel).exists()]

    analytics_manifest = {
        "generated_at": _now_iso(),
        "source_export_dir": str(source_export_dir),
        "outputs": [
            "analytics/latest/latest_video_metrics.csv",
            "analytics/latest/latest_channel_metrics.csv",
            "analytics/latest/latest_title_metrics.csv",
            "analytics/latest/latest_run_metrics.json",
            "analytics/latest/analytics_manifest.json",
        ],
        "schema_version": "analytics_v1",
        "future_feature_inputs_checked": FUTURE_FEATURE_INPUTS,
        "future_feature_inputs_found": future_found,
        "warnings": warnings,
    }

    analytics_latest.mkdir(parents=True, exist_ok=True)

    output_video = analytics_latest / "latest_video_metrics.csv"
    output_channel = analytics_latest / "latest_channel_metrics.csv"
    output_title = analytics_latest / "latest_title_metrics.csv"
    output_run = analytics_latest / "latest_run_metrics.json"
    output_manifest = analytics_latest / "analytics_manifest.json"

    _write_csv(output_video, VIDEO_METRICS_COLUMNS, video_rows)
    _write_csv(output_channel, CHANNEL_METRICS_COLUMNS, channel_rows)
    _write_csv(output_title, TITLE_METRICS_COLUMNS, title_rows)
    output_run.write_text(json.dumps(run_metrics, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    output_manifest.write_text(json.dumps(analytics_manifest, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")

    analytics_root_abs = analytics_root.resolve()
    for path in [output_video, output_channel, output_title, output_run, output_manifest]:
        if not path.resolve().is_relative_to(analytics_root_abs):
            warnings.append(f"Ruta de salida inválida fuera de data/analytics: {path}")
            result["status"] = "failed"
            return result

    result["status"] = "success"
    result["source_export_dir"] = str(source_export_dir)
    result["outputs"] = {
        "latest_video_metrics_csv": _safe_rel(output_video, data_root),
        "latest_channel_metrics_csv": _safe_rel(output_channel, data_root),
        "latest_title_metrics_csv": _safe_rel(output_title, data_root),
        "latest_run_metrics_json": _safe_rel(output_run, data_root),
        "analytics_manifest_json": _safe_rel(output_manifest, data_root),
    }
    return result
