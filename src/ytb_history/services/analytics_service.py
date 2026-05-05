"""Analytics data mart builders for latest exported run."""

from __future__ import annotations

import csv
import math
import json
import re
from calendar import monthrange
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

VIDEO_SCORES_COLUMNS = [
    "execution_date",
    "channel_id",
    "channel_name",
    "video_id",
    "title",
    "views_delta",
    "engagement_rate",
    "video_age_days",
    "metadata_changed",
    "growth_percentile",
    "engagement_percentile",
    "freshness_score",
    "growth_robust_z",
    "channel_median_views_delta",
    "video_vs_channel_median_growth",
    "relative_growth_percentile",
    "metadata_change_score",
    "alpha_score",
    "opportunity_score",
    "anomaly_score",
    "anomaly_method",
]

CHANNEL_BASELINES_COLUMNS = [
    "execution_date",
    "channel_id",
    "channel_name",
    "videos_tracked",
    "median_views_delta",
    "median_engagement_rate",
    "avg_duration_seconds",
    "shorts_ratio",
    "total_views_delta",
    "new_videos",
    "top_video_id",
    "top_video_title",
    "top_video_views_delta",
    "channel_growth_percentile",
    "channel_momentum_score",
]

VIDEO_LIFECYCLE_COLUMNS = [
    "execution_date",
    "video_id",
    "channel_id",
    "channel_name",
    "upload_date",
    "video_age_days",
    "views",
    "views_delta",
    "views_per_day_since_upload",
    "lifecycle_stage",
]

PERIOD_VIDEO_COLUMNS = [
    "grain",
    "period_start",
    "period_end",
    "video_id",
    "channel_id",
    "channel_name",
    "title",
    "period_views_delta",
    "period_likes_delta",
    "period_comments_delta",
    "period_avg_engagement_rate",
    "period_snapshots",
    "latest_views",
    "latest_likes",
    "latest_comments",
]

PERIOD_CHANNEL_COLUMNS = [
    "grain",
    "period_start",
    "period_end",
    "channel_id",
    "channel_name",
    "period_video_count",
    "period_new_videos",
    "period_views_delta",
    "period_likes_delta",
    "period_comments_delta",
    "period_avg_engagement_rate",
    "period_top_video_id",
    "period_top_video_title",
    "period_top_video_views_delta",
]

VIDEO_ADVANCED_COLUMNS = [
    "execution_date",
    "channel_id",
    "channel_name",
    "video_id",
    "title",
    "video_age_days",
    "duration_bucket",
    "views_delta",
    "likes_delta",
    "comments_delta",
    "engagement_rate",
    "comment_rate",
    "age_adjusted_views_velocity",
    "age_adjusted_growth_percentile",
    "channel_median_views_delta",
    "channel_relative_growth_ratio",
    "channel_relative_growth_log",
    "channel_relative_success_score",
    "format_median_views_delta",
    "format_relative_growth_ratio",
    "current_period_views_delta",
    "previous_period_views_delta",
    "growth_acceleration",
    "growth_acceleration_ratio",
    "growth_trend_label",
    "growth_acceleration_score",
    "peak_velocity",
    "current_velocity",
    "decay_resistance_score",
    "short_term_success_score",
    "mid_term_success_score",
    "long_term_success_score",
    "overall_success_score",
    "trend_burst_score",
    "evergreen_score",
    "packaging_problem_score",
    "metadata_changed",
    "metadata_lift_status",
    "metadata_lift_ratio",
    "growth_volatility_robust",
    "metric_confidence_score",
    "success_horizon_label",
]

CHANNEL_ADVANCED_COLUMNS = [
    "execution_date",
    "channel_id",
    "channel_name",
    "videos_tracked",
    "new_videos",
    "total_views_delta",
    "median_views_delta",
    "avg_engagement_rate",
    "channel_growth_7d",
    "channel_growth_30d",
    "channel_momentum_7d_vs_30d",
    "channel_momentum_score",
    "publish_frequency_7d",
    "publish_frequency_30d",
    "channel_consistency_score",
    "channel_volatility_robust",
    "top_video_id",
    "top_video_title",
    "top_video_views_delta",
    "shorts_ratio",
    "mid_ratio",
    "long_ratio",
    "metric_confidence_score",
]

METRIC_ELIGIBILITY_COLUMNS = [
    "execution_date",
    "video_id",
    "channel_id",
    "video_age_days",
    "short_term_eligible",
    "mid_term_eligible",
    "long_term_eligible",
    "evergreen_eligible",
    "trend_burst_eligible",
    "metadata_lift_eligible",
    "channel_baseline_eligible",
    "confidence_reason",
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


def safe_int(value: Any) -> int | None:
    return _to_int(value)


def safe_float(value: Any) -> float | None:
    if value in (None, ""):
        return None
    try:
        return float(str(value))
    except (TypeError, ValueError):
        return None


def safe_divide(numerator: float | int | None, denominator: float | int | None, default: float | None = None) -> float | None:
    num = safe_float(numerator)
    den = safe_float(denominator)
    if num is None or den is None or den == 0:
        return default
    return num / den


def _to_bool(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    if value is None:
        return False
    return str(value).strip().lower() in {"1", "true", "yes", "y"}


def _to_float_string(value: float | None) -> str:
    if value is None:
        return ""
    return f"{value:.6f}".rstrip("0").rstrip(".")


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


def _first_capture_growth_files_by_day(exports_root: Path) -> list[Path]:
    first_by_day: dict[str, Path] = {}
    for csv_path in sorted(exports_root.glob("dt=*/run=*/video_growth_summary.csv")):
        day_token = next((part for part in csv_path.parts if part.startswith("dt=")), "")
        if not day_token or day_token in first_by_day:
            continue
        first_by_day[day_token] = csv_path
    return [first_by_day[day] for day in sorted(first_by_day)]


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


def median_safe(values: list[float | None]) -> float | None:
    valid = sorted(value for value in values if value is not None)
    if not valid:
        return None
    return float(median(valid))


def mad_safe(values: list[float | None]) -> float | None:
    med = median_safe(values)
    if med is None:
        return None
    abs_deviations = [abs(value - med) for value in values if value is not None]
    if not abs_deviations:
        return None
    return float(median(abs_deviations))


def percentile_rank_0_100(values: list[float | None]) -> list[str]:
    valid_values = [value for value in values if value is not None]
    if not valid_values:
        return [""] * len(values)
    if len(set(valid_values)) == 1:
        return ["50" if value is not None else "" for value in values]

    sorted_valid = sorted(valid_values)
    n = len(sorted_valid)
    percentiles: list[str] = []
    for value in values:
        if value is None:
            percentiles.append("")
            continue
        less = sum(1 for item in sorted_valid if item < value)
        equal = sum(1 for item in sorted_valid if item == value)
        rank = less + (equal + 1) / 2
        score = ((rank - 1) / (n - 1)) * 100 if n > 1 else 50.0
        percentiles.append(_to_float_string(score))
    return percentiles


def percentile_rank(values: list[float | None]) -> list[str]:
    return percentile_rank_0_100(values)


def robust_z_scores(values: list[float | None]) -> list[str]:
    med = median_safe(values)
    mad = mad_safe(values)
    if med is None:
        return [""] * len(values)
    if mad in (None, 0):
        return ["0" if value is not None else "" for value in values]

    output: list[str] = []
    for value in values:
        if value is None:
            output.append("")
            continue
        robust_z = 0.6745 * (value - med) / mad
        output.append(_to_float_string(robust_z))
    return output


def robust_z_score(value: float | None, med: float | None, mad: float | None) -> float | None:
    if value is None or med is None:
        return None
    if mad in (None, 0):
        return 0.0
    return 0.6745 * (value - med) / mad


def log1p_safe(value: float | int | None) -> float | None:
    parsed = safe_float(value)
    if parsed is None or parsed < -1:
        return None
    return math.log1p(parsed)


def safe_log1p(value: float | int | None) -> float | None:
    return log1p_safe(value)


def iqr_safe(values: list[float | None]) -> float | None:
    valid = sorted(value for value in values if value is not None)
    if len(valid) < 2:
        return None
    q1_idx = int(0.25 * (len(valid) - 1))
    q3_idx = int(0.75 * (len(valid) - 1))
    return valid[q3_idx] - valid[q1_idx]


def cap_score(value: float | None, min_value: float = 0, max_value: float = 100) -> float | None:
    if value is None:
        return None
    return max(min_value, min(max_value, value))


def cap_0_100(value: float | None) -> float | None:
    return cap_score(value, 0, 100)


def truthy(value: Any) -> bool:
    return _to_bool(value)


def empty_if_not_eligible(value: float | str | None, eligible: bool) -> str:
    if not eligible or value is None:
        return ""
    return value if isinstance(value, str) else _to_float_string(value)


def _lifecycle_stage(video_age_days: float | None) -> str:
    if video_age_days is None:
        return "unknown"
    if video_age_days < 1:
        return "first_24h"
    if video_age_days < 7:
        return "early"
    if video_age_days < 30:
        return "active"
    if video_age_days < 90:
        return "mature"
    return "long_tail"


def _period_bounds(execution_date: datetime, grain: str) -> tuple[str, str]:
    day = execution_date.date()
    if grain == "daily":
        period_start = day
        period_end = day
    elif grain == "weekly":
        weekday = day.weekday()
        period_start = day.fromordinal(day.toordinal() - weekday)
        period_end = day.fromordinal(period_start.toordinal() + 6)
    elif grain == "monthly":
        period_start = day.replace(day=1)
        last_day = monthrange(day.year, day.month)[1]
        period_end = day.replace(day=last_day)
    else:
        raise ValueError(f"Unsupported grain: {grain}")
    return period_start.isoformat(), period_end.isoformat()


def build_period_aggregations(rows: list[dict[str, str]], *, grain: str) -> tuple[list[dict[str, str]], list[dict[str, str]]]:
    video_acc: dict[tuple[str, str, str], dict[str, Any]] = {}
    for row in rows:
        execution_dt = _parse_iso8601(row.get("execution_date"))
        if execution_dt is None:
            continue
        period_start, period_end = _period_bounds(execution_dt.astimezone(timezone.utc), grain)
        key = (period_start, period_end, str(row.get("video_id", "")))
        acc = video_acc.setdefault(
            key,
            {
                "grain": grain,
                "period_start": period_start,
                "period_end": period_end,
                "video_id": str(row.get("video_id", "")),
                "channel_id": str(row.get("channel_id", "")),
                "channel_name": str(row.get("channel_name", "")),
                "title": str(row.get("title", "")),
                "period_views_delta": 0,
                "period_likes_delta": 0,
                "period_comments_delta": 0,
                "engagement_values": [],
                "period_snapshots": 0,
                "latest_views": None,
                "latest_likes": None,
                "latest_comments": None,
                "latest_execution_dt": None,
                "period_new_videos": 0,
            },
        )
        views_delta = safe_int(row.get("views_delta"))
        likes_delta = safe_int(row.get("likes_delta"))
        comments_delta = safe_int(row.get("comments_delta"))
        if views_delta is not None:
            acc["period_views_delta"] += views_delta
        if likes_delta is not None:
            acc["period_likes_delta"] += likes_delta
        if comments_delta is not None:
            acc["period_comments_delta"] += comments_delta
        engagement = safe_float(row.get("engagement_rate"))
        if engagement is not None:
            acc["engagement_values"].append(engagement)
        acc["period_snapshots"] += 1
        acc["period_new_videos"] += 1 if _to_bool(row.get("is_new_video")) else 0

        if acc["latest_execution_dt"] is None or execution_dt > acc["latest_execution_dt"]:
            acc["latest_execution_dt"] = execution_dt
            acc["latest_views"] = safe_int(row.get("views"))
            acc["latest_likes"] = safe_int(row.get("likes"))
            acc["latest_comments"] = safe_int(row.get("comments"))
            if row.get("channel_name"):
                acc["channel_name"] = str(row.get("channel_name", ""))
            if row.get("title"):
                acc["title"] = str(row.get("title", ""))

    video_rows: list[dict[str, str]] = []
    for acc in video_acc.values():
        avg_engagement = (
            sum(acc["engagement_values"]) / len(acc["engagement_values"]) if acc["engagement_values"] else None
        )
        video_rows.append(
            {
                "grain": acc["grain"],
                "period_start": acc["period_start"],
                "period_end": acc["period_end"],
                "video_id": acc["video_id"],
                "channel_id": acc["channel_id"],
                "channel_name": acc["channel_name"],
                "title": acc["title"],
                "period_views_delta": str(acc["period_views_delta"]),
                "period_likes_delta": str(acc["period_likes_delta"]),
                "period_comments_delta": str(acc["period_comments_delta"]),
                "period_avg_engagement_rate": _to_float_string(avg_engagement),
                "period_snapshots": str(acc["period_snapshots"]),
                "latest_views": "" if acc["latest_views"] is None else str(acc["latest_views"]),
                "latest_likes": "" if acc["latest_likes"] is None else str(acc["latest_likes"]),
                "latest_comments": "" if acc["latest_comments"] is None else str(acc["latest_comments"]),
            }
        )

    channel_acc: dict[tuple[str, str, str], dict[str, Any]] = {}
    for acc in video_acc.values():
        key = (acc["period_start"], acc["period_end"], acc["channel_id"])
        channel = channel_acc.setdefault(
            key,
            {
                "grain": grain,
                "period_start": acc["period_start"],
                "period_end": acc["period_end"],
                "channel_id": acc["channel_id"],
                "channel_name": acc["channel_name"],
                "period_video_count": 0,
                "period_new_videos": 0,
                "period_views_delta": 0,
                "period_likes_delta": 0,
                "period_comments_delta": 0,
                "engagement_values": [],
                "period_top_video_id": "",
                "period_top_video_title": "",
                "period_top_video_views_delta": None,
            },
        )
        channel["period_video_count"] += 1
        channel["period_new_videos"] += acc["period_new_videos"]
        channel["period_views_delta"] += acc["period_views_delta"]
        channel["period_likes_delta"] += acc["period_likes_delta"]
        channel["period_comments_delta"] += acc["period_comments_delta"]
        channel["engagement_values"].extend(acc["engagement_values"])
        if (
            channel["period_top_video_views_delta"] is None
            or acc["period_views_delta"] > channel["period_top_video_views_delta"]
        ):
            channel["period_top_video_views_delta"] = acc["period_views_delta"]
            channel["period_top_video_id"] = acc["video_id"]
            channel["period_top_video_title"] = acc["title"]

    channel_rows: list[dict[str, str]] = []
    for channel in channel_acc.values():
        avg_engagement = (
            sum(channel["engagement_values"]) / len(channel["engagement_values"]) if channel["engagement_values"] else None
        )
        channel_rows.append(
            {
                "grain": channel["grain"],
                "period_start": channel["period_start"],
                "period_end": channel["period_end"],
                "channel_id": channel["channel_id"],
                "channel_name": channel["channel_name"],
                "period_video_count": str(channel["period_video_count"]),
                "period_new_videos": str(channel["period_new_videos"]),
                "period_views_delta": str(channel["period_views_delta"]),
                "period_likes_delta": str(channel["period_likes_delta"]),
                "period_comments_delta": str(channel["period_comments_delta"]),
                "period_avg_engagement_rate": _to_float_string(avg_engagement),
                "period_top_video_id": channel["period_top_video_id"],
                "period_top_video_title": channel["period_top_video_title"],
                "period_top_video_views_delta": (
                    "" if channel["period_top_video_views_delta"] is None else str(channel["period_top_video_views_delta"])
                ),
            }
        )

    video_rows.sort(
        key=lambda row: (
            row["period_start"],
            safe_int(row["period_views_delta"]) or 0,
        ),
        reverse=True,
    )
    channel_rows.sort(
        key=lambda row: (
            row["period_start"],
            safe_int(row["period_views_delta"]) or 0,
        ),
        reverse=True,
    )
    return video_rows, channel_rows


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

    growth_rank_values = [safe_int(row["views_delta"]) for row in video_rows]
    engagement_rank_values = [safe_float(row["engagement_rate"]) for row in video_rows]
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

        engagement_value = safe_float(row["engagement_rate"])
        if engagement_value is not None:
            channel["engagement_values"].append(engagement_value)
        views_delta_value = safe_int(row["views_delta"])
        if views_delta_value is not None:
            val = views_delta_value
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

    channel_median_views_delta: dict[str, float | None] = {}
    for channel_id in channel_acc:
        channel_values = [safe_float(row["views_delta"]) for row in video_rows if row["channel_id"] == channel_id]
        channel_median_views_delta[channel_id] = median_safe(channel_values)

    growth_percentiles = percentile_rank([safe_float(row["views_delta"]) for row in video_rows])
    engagement_percentiles = percentile_rank([safe_float(row["engagement_rate"]) for row in video_rows])
    log_growth_values = [safe_log1p(max(safe_float(row["views_delta"]) or 0, 0)) for row in video_rows]
    growth_robust_z = robust_z_scores(log_growth_values)

    relative_growth_values: list[float | None] = []
    for row in video_rows:
        views_delta = safe_float(row["views_delta"])
        channel_median = channel_median_views_delta.get(row["channel_id"])
        if views_delta is None:
            relative_growth_values.append(None)
            continue
        relative_growth_values.append(views_delta / max(channel_median or 0, 1))
    relative_growth_percentiles = percentile_rank(relative_growth_values)

    video_scores_rows: list[dict[str, str]] = []
    for idx, row in enumerate(video_rows):
        growth_percentile = safe_float(growth_percentiles[idx])
        engagement_percentile = safe_float(engagement_percentiles[idx])
        relative_growth_percentile = safe_float(relative_growth_percentiles[idx])
        video_age_days = safe_float(row["video_age_days"])
        freshness_score = 100 * math.exp(-(video_age_days or 0) / 7)
        metadata_change_score = 100.0 if _to_bool(row["metadata_changed"]) else 0.0

        alpha_score = None
        if growth_percentile is not None and relative_growth_percentile is not None and engagement_percentile is not None:
            alpha_score = (
                0.35 * growth_percentile
                + 0.20 * relative_growth_percentile
                + 0.20 * engagement_percentile
                + 0.15 * freshness_score
                + 0.10 * metadata_change_score
            )

        opportunity_score = None
        if alpha_score is not None and relative_growth_percentile is not None and engagement_percentile is not None:
            opportunity_score = (
                0.50 * alpha_score
                + 0.25 * relative_growth_percentile
                + 0.15 * engagement_percentile
                + 0.10 * freshness_score
            )

        robust_z = safe_float(growth_robust_z[idx])
        anomaly_score = cap_score((abs(robust_z) / 5) * 100 if robust_z is not None else None)
        channel_median = channel_median_views_delta.get(row["channel_id"])
        relative_growth = relative_growth_values[idx]

        video_scores_rows.append(
            {
                "execution_date": row["execution_date"],
                "channel_id": row["channel_id"],
                "channel_name": row["channel_name"],
                "video_id": row["video_id"],
                "title": row["title"],
                "views_delta": row["views_delta"],
                "engagement_rate": row["engagement_rate"],
                "video_age_days": row["video_age_days"],
                "metadata_changed": row["metadata_changed"],
                "growth_percentile": growth_percentiles[idx],
                "engagement_percentile": engagement_percentiles[idx],
                "freshness_score": _to_float_string(freshness_score),
                "growth_robust_z": growth_robust_z[idx],
                "channel_median_views_delta": _to_float_string(channel_median),
                "video_vs_channel_median_growth": _to_float_string(relative_growth),
                "relative_growth_percentile": relative_growth_percentiles[idx],
                "metadata_change_score": _to_float_string(metadata_change_score),
                "alpha_score": _to_float_string(alpha_score),
                "opportunity_score": _to_float_string(opportunity_score),
                "anomaly_score": _to_float_string(anomaly_score),
                "anomaly_method": "robust_z",
            }
        )

    channel_growth_percentiles = percentile_rank([safe_float(row["total_views_delta"]) for row in channel_rows])
    channel_baselines_rows: list[dict[str, str]] = []
    for idx, channel in enumerate(channel_rows):
        channel_id = channel["channel_id"]
        videos_for_channel = [row for row in video_rows if row["channel_id"] == channel_id]
        median_engagement = median_safe([safe_float(row["engagement_rate"]) for row in videos_for_channel])
        avg_duration = None
        valid_duration = [safe_float(row["duration_seconds"]) for row in videos_for_channel if safe_float(row["duration_seconds"]) is not None]
        if valid_duration:
            avg_duration = sum(valid_duration) / len(valid_duration)
        shorts_count = sum(1 for row in videos_for_channel if _to_bool(row["is_short"]))
        videos_tracked = safe_int(channel["videos_tracked"]) or 0
        shorts_ratio = (shorts_count / videos_tracked) if videos_tracked else None
        channel_growth_percentile = channel_growth_percentiles[idx]
        channel_baselines_rows.append(
            {
                "execution_date": channel["execution_date"],
                "channel_id": channel_id,
                "channel_name": channel["channel_name"],
                "videos_tracked": channel["videos_tracked"],
                "median_views_delta": _to_float_string(channel_median_views_delta.get(channel_id)),
                "median_engagement_rate": _to_float_string(median_engagement),
                "avg_duration_seconds": _to_float_string(avg_duration),
                "shorts_ratio": _to_float_string(shorts_ratio),
                "total_views_delta": channel["total_views_delta"],
                "new_videos": channel["new_videos"],
                "top_video_id": channel["top_video_id"],
                "top_video_title": channel["top_video_title"],
                "top_video_views_delta": channel["top_video_views_delta"],
                "channel_growth_percentile": channel_growth_percentile,
                "channel_momentum_score": channel_growth_percentile,
            }
        )

    video_lifecycle_rows: list[dict[str, str]] = []
    for row in video_rows:
        age_days = safe_float(row["video_age_days"])
        views = safe_float(row["views"])
        views_per_day = None
        if views is not None and age_days is not None:
            views_per_day = views / max(age_days, 1 / 24)
        video_lifecycle_rows.append(
            {
                "execution_date": row["execution_date"],
                "video_id": row["video_id"],
                "channel_id": row["channel_id"],
                "channel_name": row["channel_name"],
                "upload_date": row["upload_date"],
                "video_age_days": row["video_age_days"],
                "views": row["views"],
                "views_delta": row["views_delta"],
                "views_per_day_since_upload": _to_float_string(views_per_day),
                "lifecycle_stage": _lifecycle_stage(age_days),
            }
        )

    historical_growth_files = _first_capture_growth_files_by_day(exports_root)
    if not historical_growth_files:
        historical_growth_files = [required_files["video_growth_summary"]]
    historical_rows: list[dict[str, str]] = []
    for csv_path in historical_growth_files:
        historical_rows.extend(_read_csv(csv_path))

    period_outputs: dict[str, dict[str, list[dict[str, str]]]] = {}
    for grain in ["daily", "weekly", "monthly"]:
        grain_video_rows, grain_channel_rows = build_period_aggregations(historical_rows, grain=grain)
        period_outputs[grain] = {
            "video_rows": grain_video_rows,
            "channel_rows": grain_channel_rows,
        }

    historical_rows_sorted = sorted(
        historical_rows,
        key=lambda row: (_parse_iso8601(row.get("execution_date")) or datetime.min.replace(tzinfo=timezone.utc)),
    )
    history_by_video: dict[str, list[dict[str, str]]] = {}
    history_by_channel: dict[str, list[dict[str, str]]] = {}
    for row in historical_rows_sorted:
        history_by_video.setdefault(str(row.get("video_id", "")), []).append(row)
        history_by_channel.setdefault(str(row.get("channel_id", "")), []).append(row)

    latest_execution_dt = _parse_iso8601(video_rows[0]["execution_date"] if video_rows else None)
    latest_execution_dt = latest_execution_dt.astimezone(timezone.utc) if latest_execution_dt else None

    engagement_percentiles = percentile_rank_0_100([safe_float(row.get("engagement_rate")) for row in video_rows])
    comment_rate_percentiles = percentile_rank_0_100([safe_float(row.get("comment_rate")) for row in video_rows])
    views_per_day_percentiles = percentile_rank_0_100([safe_float(row.get("views_per_day_since_upload")) for row in video_rows])
    growth_percentiles_video = percentile_rank_0_100([safe_float(row.get("views_delta")) for row in video_rows])

    channel_median_engagement: dict[str, float | None] = {}
    for channel_id in channel_acc:
        channel_median_engagement[channel_id] = median_safe(
            [safe_float(row.get("engagement_rate")) for row in video_rows if row.get("channel_id") == channel_id]
        )
    channel_relative_engagement_values: list[float | None] = []
    for row in video_rows:
        engagement = safe_float(row.get("engagement_rate"))
        base = channel_median_engagement.get(row["channel_id"])
        channel_relative_engagement_values.append(safe_divide(engagement, max(base or 0, 1e-6)))
    channel_relative_engagement_percentiles = percentile_rank_0_100(channel_relative_engagement_values)

    freshness_scores: dict[str, float] = {}
    for row in video_scores_rows:
        freshness_scores[str(row["video_id"])] = safe_float(row.get("freshness_score")) or 0.0

    format_medians = {
        bucket: median_safe([safe_float(row.get("views_delta")) for row in video_rows if row.get("duration_bucket") == bucket])
        for bucket in {"short", "mid", "long", "unknown"}
    }

    age_adjusted_velocity_values: list[float | None] = []
    current_period_values: list[float | None] = []
    previous_period_values: list[float | None] = []
    growth_acc_values: list[float | None] = []
    channel_relative_growth_log_values: list[float | None] = []
    video_meta: dict[str, dict[str, Any]] = {}

    for row in video_rows:
        video_id = str(row["video_id"])
        execution_dt = _parse_iso8601(row.get("execution_date"))
        views_delta = safe_float(row.get("views_delta"))
        history = history_by_video.get(video_id, [])
        prev_row = None
        if execution_dt is not None:
            for item in reversed(history):
                item_dt = _parse_iso8601(item.get("execution_date"))
                if item_dt is not None and item_dt < execution_dt:
                    prev_row = item
                    break
        prev_views_delta = safe_float(prev_row.get("views_delta")) if prev_row else None
        days_since_prev = None
        if prev_row and execution_dt:
            prev_dt = _parse_iso8601(prev_row.get("execution_date"))
            if prev_dt:
                days_since_prev = max((execution_dt - prev_dt).total_seconds() / 86400, 1 / 24)
        if days_since_prev is None:
            days_since_prev = max(safe_float(row.get("video_age_days")) or 0, 1 / 24)
        age_velocity = safe_divide(views_delta, days_since_prev)

        channel_median = channel_median_views_delta.get(str(row["channel_id"]))
        channel_relative_log = None
        if views_delta is not None:
            channel_relative_log = (log1p_safe(max(views_delta, 0)) or 0) - (log1p_safe(max(channel_median or 0, 0)) or 0)
        growth_acc = (views_delta - prev_views_delta) if views_delta is not None and prev_views_delta is not None else None

        age_adjusted_velocity_values.append(age_velocity)
        current_period_values.append(views_delta)
        previous_period_values.append(prev_views_delta)
        growth_acc_values.append(growth_acc)
        channel_relative_growth_log_values.append(channel_relative_log)
        video_meta[video_id] = {
            "prev_views_delta": prev_views_delta,
            "days_since_prev": days_since_prev,
            "age_velocity": age_velocity,
            "growth_acc": growth_acc,
            "history": history,
        }

    age_adjusted_growth_percentiles = percentile_rank_0_100([log1p_safe(max(value, 0)) if value is not None else None for value in age_adjusted_velocity_values])
    channel_relative_success_scores = percentile_rank_0_100(channel_relative_growth_log_values)
    growth_acc_scores = percentile_rank_0_100([log1p_safe(max(value, 0)) if value is not None else None for value in growth_acc_values])

    video_advanced_rows: list[dict[str, str]] = []
    eligibility_rows: list[dict[str, str]] = []
    for idx, row in enumerate(video_rows):
        video_id = str(row["video_id"])
        channel_id = str(row["channel_id"])
        channel_count = len([item for item in video_rows if item["channel_id"] == channel_id])
        meta = video_meta[video_id]
        history = meta["history"]
        views_delta = safe_float(row.get("views_delta"))
        prev_views_delta = previous_period_values[idx]
        growth_acc = growth_acc_values[idx]
        growth_acc_ratio = safe_divide(views_delta, max(prev_views_delta or 0, 1)) if prev_views_delta is not None else None
        if growth_acc_ratio is None:
            trend_label = "unknown"
        elif growth_acc_ratio >= 2:
            trend_label = "accelerating"
        elif growth_acc_ratio >= 0.8:
            trend_label = "stable"
        elif growth_acc_ratio >= 0.2:
            trend_label = "decelerating"
        else:
            trend_label = "fading"

        channel_median = channel_median_views_delta.get(channel_id)
        channel_rel_ratio = safe_divide(views_delta, max(channel_median or 0, 1))
        format_median = format_medians.get(row["duration_bucket"])
        format_rel_ratio = safe_divide(views_delta, max(format_median or 0, 1))
        velocity_history = []
        for h_idx, h_row in enumerate(history):
            h_views_delta = safe_float(h_row.get("views_delta"))
            h_dt = _parse_iso8601(h_row.get("execution_date"))
            h_prev_dt = _parse_iso8601(history[h_idx - 1].get("execution_date")) if h_idx > 0 else None
            h_days = max((h_dt - h_prev_dt).total_seconds() / 86400, 1 / 24) if h_dt and h_prev_dt else None
            if h_days is None:
                h_exec = _parse_iso8601(h_row.get("execution_date"))
                h_upload = _parse_iso8601(h_row.get("upload_date"))
                h_age = ((h_exec - h_upload).total_seconds() / 86400) if h_exec and h_upload else None
                h_days = max(h_age or 0, 1 / 24)
            velocity_history.append(safe_divide(h_views_delta, h_days))
        peak_velocity = max([v for v in velocity_history if v is not None], default=None)
        current_velocity = age_adjusted_velocity_values[idx]
        decay_resistance = cap_0_100(min(safe_divide(current_velocity, max(peak_velocity or 0, 1), 0) or 0, 1) * 100) if peak_velocity is not None else None

        age_days = safe_float(row.get("video_age_days"))
        short_eligible = age_days is not None and age_days <= 3
        mid_eligible = age_days is not None and 4 <= age_days <= 30
        long_eligible = age_days is not None and 31 <= age_days <= 180
        evergreen_eligible = age_days is not None and age_days >= 30
        trend_burst_eligible = age_days is not None and age_days <= 7

        short_term = None
        if short_eligible:
            short_term = (
                0.45 * (safe_float(age_adjusted_growth_percentiles[idx]) or 0)
                + 0.25 * (safe_float(channel_relative_success_scores[idx]) or 0)
                + 0.20 * (safe_float(engagement_percentiles[idx]) or 0)
                + 0.10 * (safe_float(comment_rate_percentiles[idx]) or 0)
            )
        mid_term = None
        if mid_eligible:
            mid_term = (
                0.40 * (safe_float(age_adjusted_growth_percentiles[idx]) or 0)
                + 0.25 * (safe_float(channel_relative_success_scores[idx]) or 0)
                + 0.20 * (safe_float(engagement_percentiles[idx]) or 0)
                + 0.15 * (decay_resistance or 0)
            )
        long_term = None
        if long_eligible:
            long_term = (
                0.45 * (safe_float(age_adjusted_growth_percentiles[idx]) or 0)
                + 0.25 * (safe_float(views_per_day_percentiles[idx]) or 0)
                + 0.20 * (decay_resistance or 0)
                + 0.10 * (safe_float(engagement_percentiles[idx]) or 0)
            )

        available_scores = [score for score in [short_term, mid_term, long_term] if score is not None]
        overall = (sum(available_scores) / len(available_scores)) if available_scores else None

        trend_burst = None
        if trend_burst_eligible:
            burst_base = short_term if short_term is not None else (safe_float(age_adjusted_growth_percentiles[idx]) or 0)
            trend_burst = (
                0.45 * burst_base
                + 0.25 * (safe_float(growth_acc_scores[idx]) or 0)
                + 0.20 * freshness_scores.get(video_id, 0)
                + 0.10 * (safe_float(comment_rate_percentiles[idx]) or 0)
            )

        evergreen = None
        if evergreen_eligible:
            evergreen = (
                0.50 * (safe_float(age_adjusted_growth_percentiles[idx]) or 0)
                + 0.25 * (decay_resistance or 0)
                + 0.15 * (safe_float(views_per_day_percentiles[idx]) or 0)
                + 0.10 * (safe_float(engagement_percentiles[idx]) or 0)
            )

        packaging_problem = cap_0_100(
            (
                0.50 * (safe_float(engagement_percentiles[idx]) or 0)
                + 0.30 * (safe_float(comment_rate_percentiles[idx]) or 0)
                + 0.20 * (safe_float(channel_relative_engagement_percentiles[idx]) or 0)
                - 0.40 * (safe_float(growth_percentiles_video[idx]) or 0)
            )
        )

        metadata_changed = truthy(row.get("metadata_changed"))
        metadata_lift_ratio = None
        metadata_lift_status = "insufficient_history"
        if metadata_changed and len(history) >= 3:
            change_idx = next((i for i, h in enumerate(history) if truthy(h.get("title_changed")) or truthy(h.get("description_changed")) or truthy(h.get("tags_changed"))), None)
            if change_idx is not None and change_idx > 0 and change_idx < len(velocity_history) - 1:
                pre = [v for v in velocity_history[:change_idx] if v is not None]
                post = [v for v in velocity_history[change_idx:] if v is not None]
                pre_med = median_safe(pre)
                post_med = median_safe(post)
                metadata_lift_ratio = safe_divide(post_med, max(pre_med or 0, 1))
                if metadata_lift_ratio is not None:
                    if metadata_lift_ratio >= 1.5:
                        metadata_lift_status = "positive_lift"
                    elif metadata_lift_ratio >= 0.8:
                        metadata_lift_status = "neutral"
                    else:
                        metadata_lift_status = "negative_lift"

        history_deltas = [safe_float(h.get("views_delta")) for h in history]
        growth_volatility = None
        if len([v for v in history_deltas if v is not None]) >= 3:
            growth_volatility = safe_divide(iqr_safe(history_deltas), max(median_safe(history_deltas) or 0, 1))

        completeness_fields = [
            row.get("views"),
            row.get("views_delta"),
            row.get("engagement_rate"),
            row.get("video_age_days"),
            row.get("duration_bucket"),
            row.get("channel_id"),
        ]
        completeness = (sum(1 for value in completeness_fields if value not in (None, "")) / len(completeness_fields)) * 100
        history_depth = min(len(history) / 10, 1) * 100
        baseline_size = min(channel_count / 20, 1) * 100
        recency = 100 if latest_execution_dt and _parse_iso8601(row.get("execution_date")) == latest_execution_dt else 50
        metric_confidence = 0.35 * history_depth + 0.25 * completeness + 0.20 * baseline_size + 0.20 * recency

        if age_days is None:
            horizon_label = "unknown"
        elif age_days <= 3:
            horizon_label = "short_term"
        elif age_days <= 30:
            horizon_label = "mid_term"
        elif age_days <= 180:
            horizon_label = "long_term"
        else:
            horizon_label = "mature_archive"

        channel_baseline_eligible = channel_count >= 3
        metadata_lift_eligible = metadata_lift_ratio is not None
        if age_days is None:
            confidence_reason = "missing_age"
        elif len(history) < 3:
            confidence_reason = "low_history"
        elif not channel_baseline_eligible:
            confidence_reason = "insufficient_channel_baseline"
        else:
            confidence_reason = "ok"

        eligibility_rows.append(
            {
                "execution_date": row["execution_date"],
                "video_id": video_id,
                "channel_id": channel_id,
                "video_age_days": row["video_age_days"],
                "short_term_eligible": str(short_eligible),
                "mid_term_eligible": str(mid_eligible),
                "long_term_eligible": str(long_eligible),
                "evergreen_eligible": str(evergreen_eligible),
                "trend_burst_eligible": str(trend_burst_eligible),
                "metadata_lift_eligible": str(metadata_lift_eligible),
                "channel_baseline_eligible": str(channel_baseline_eligible),
                "confidence_reason": confidence_reason,
            }
        )

        video_advanced_rows.append(
            {
                "execution_date": row["execution_date"],
                "channel_id": channel_id,
                "channel_name": row["channel_name"],
                "video_id": video_id,
                "title": row["title"],
                "video_age_days": row["video_age_days"],
                "duration_bucket": row["duration_bucket"],
                "views_delta": row["views_delta"],
                "likes_delta": row["likes_delta"],
                "comments_delta": row["comments_delta"],
                "engagement_rate": row["engagement_rate"],
                "comment_rate": row["comment_rate"],
                "age_adjusted_views_velocity": _to_float_string(age_adjusted_velocity_values[idx]),
                "age_adjusted_growth_percentile": age_adjusted_growth_percentiles[idx],
                "channel_median_views_delta": _to_float_string(channel_median),
                "channel_relative_growth_ratio": _to_float_string(channel_rel_ratio),
                "channel_relative_growth_log": _to_float_string(channel_relative_growth_log_values[idx]),
                "channel_relative_success_score": channel_relative_success_scores[idx],
                "format_median_views_delta": _to_float_string(format_median),
                "format_relative_growth_ratio": _to_float_string(format_rel_ratio),
                "current_period_views_delta": _to_float_string(current_period_values[idx]),
                "previous_period_views_delta": _to_float_string(previous_period_values[idx]),
                "growth_acceleration": _to_float_string(growth_acc_values[idx]),
                "growth_acceleration_ratio": _to_float_string(growth_acc_ratio),
                "growth_trend_label": trend_label,
                "growth_acceleration_score": growth_acc_scores[idx],
                "peak_velocity": _to_float_string(peak_velocity),
                "current_velocity": _to_float_string(current_velocity),
                "decay_resistance_score": _to_float_string(decay_resistance),
                "short_term_success_score": empty_if_not_eligible(short_term, short_eligible),
                "mid_term_success_score": empty_if_not_eligible(mid_term, mid_eligible),
                "long_term_success_score": empty_if_not_eligible(long_term, long_eligible),
                "overall_success_score": _to_float_string(overall),
                "trend_burst_score": empty_if_not_eligible(trend_burst, trend_burst_eligible),
                "evergreen_score": empty_if_not_eligible(evergreen, evergreen_eligible),
                "packaging_problem_score": _to_float_string(packaging_problem),
                "metadata_changed": str(metadata_changed),
                "metadata_lift_status": metadata_lift_status,
                "metadata_lift_ratio": _to_float_string(metadata_lift_ratio),
                "growth_volatility_robust": _to_float_string(growth_volatility),
                "metric_confidence_score": _to_float_string(metric_confidence),
                "success_horizon_label": horizon_label,
            }
        )

    channel_momentum_values: list[float | None] = []
    channel_advanced_tmp: list[dict[str, Any]] = []
    for channel in channel_rows:
        channel_id = channel["channel_id"]
        history = history_by_channel.get(channel_id, [])
        ch_growth_7 = 0.0
        ch_growth_30 = 0.0
        publish_7 = 0
        publish_30 = 0
        for h in history:
            h_dt = _parse_iso8601(h.get("execution_date"))
            if latest_execution_dt is None or h_dt is None:
                continue
            days = (latest_execution_dt - h_dt.astimezone(timezone.utc)).total_seconds() / 86400
            if 0 <= days <= 30:
                ch_growth_30 += safe_float(h.get("views_delta")) or 0
                publish_30 += 1 if truthy(h.get("is_new_video")) else 0
            if 0 <= days <= 7:
                ch_growth_7 += safe_float(h.get("views_delta")) or 0
                publish_7 += 1 if truthy(h.get("is_new_video")) else 0
        momentum = safe_divide(ch_growth_7, max(((ch_growth_30 / 30) * 7), 1))
        channel_momentum_values.append(momentum)
        channel_deltas = [safe_float(h.get("views_delta")) for h in history]
        volatility = safe_divide(iqr_safe(channel_deltas), max(median_safe(channel_deltas) or 0, 1))
        channel_advanced_tmp.append(
            {
                "channel_id": channel_id,
                "channel": channel,
                "history": history,
                "channel_growth_7d": ch_growth_7,
                "channel_growth_30d": ch_growth_30,
                "publish_frequency_7d": publish_7,
                "publish_frequency_30d": publish_30,
                "channel_momentum_7d_vs_30d": momentum,
                "channel_volatility_robust": volatility,
            }
        )

    channel_momentum_scores = percentile_rank_0_100(channel_momentum_values)
    channel_consistency_scores = percentile_rank_0_100([item["channel_volatility_robust"] for item in channel_advanced_tmp])
    channel_advanced_rows: list[dict[str, str]] = []
    for idx, item in enumerate(channel_advanced_tmp):
        channel = item["channel"]
        videos_tracked = safe_float(channel.get("videos_tracked")) or 0
        shorts = safe_float(channel.get("shorts_count")) or 0
        mid = safe_float(channel.get("mid_count")) or 0
        long = safe_float(channel.get("long_count")) or 0
        completeness_fields = [
            channel.get("videos_tracked"),
            channel.get("total_views_delta"),
            channel.get("avg_engagement_rate"),
            channel.get("median_views_delta"),
        ]
        completeness = (sum(1 for value in completeness_fields if value not in (None, "")) / len(completeness_fields)) * 100
        history_depth = min(len(item["history"]) / 20, 1) * 100
        video_depth = min(videos_tracked / 20, 1) * 100
        recency = 100
        channel_confidence = 0.35 * video_depth + 0.25 * history_depth + 0.20 * completeness + 0.20 * recency
        channel_advanced_rows.append(
            {
                "execution_date": channel["execution_date"],
                "channel_id": channel["channel_id"],
                "channel_name": channel["channel_name"],
                "videos_tracked": channel["videos_tracked"],
                "new_videos": channel["new_videos"],
                "total_views_delta": channel["total_views_delta"],
                "median_views_delta": channel["median_views_delta"],
                "avg_engagement_rate": channel["avg_engagement_rate"],
                "channel_growth_7d": _to_float_string(item["channel_growth_7d"]),
                "channel_growth_30d": _to_float_string(item["channel_growth_30d"]),
                "channel_momentum_7d_vs_30d": _to_float_string(item["channel_momentum_7d_vs_30d"]),
                "channel_momentum_score": channel_momentum_scores[idx],
                "publish_frequency_7d": str(item["publish_frequency_7d"]),
                "publish_frequency_30d": str(item["publish_frequency_30d"]),
                "channel_consistency_score": _to_float_string(100 - (safe_float(channel_consistency_scores[idx]) or 0)),
                "channel_volatility_robust": _to_float_string(item["channel_volatility_robust"]),
                "top_video_id": channel["top_video_id"],
                "top_video_title": channel["top_video_title"],
                "top_video_views_delta": channel["top_video_views_delta"],
                "shorts_ratio": _to_float_string(safe_divide(shorts, videos_tracked)),
                "mid_ratio": _to_float_string(safe_divide(mid, videos_tracked)),
                "long_ratio": _to_float_string(safe_divide(long, videos_tracked)),
                "metric_confidence_score": _to_float_string(channel_confidence),
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

    dashboard_index = {
        "generated_at": _now_iso(),
        "source_export_dir": str(source_export_dir),
        "analytics_manifest_path": "data/analytics/latest/analytics_manifest.json",
        "schema_version": "analytics_v1",
        "dashboard_ready": True,
        "primary_tables": {
            "latest_video_metrics": {
                "path": "data/analytics/latest/latest_video_metrics.csv",
                "description": "Métricas principales por video para la última corrida.",
                "recommended_default_sort": "views_delta desc",
                "recommended_use": "Vista de videos y ranking de crecimiento por video.",
            },
            "latest_channel_metrics": {
                "path": "data/analytics/latest/latest_channel_metrics.csv",
                "description": "Resumen agregado por canal para la última corrida.",
                "recommended_default_sort": "total_views_delta desc",
                "recommended_use": "Vista de canales y comparación de desempeño agregado.",
            },
            "latest_title_metrics": {
                "path": "data/analytics/latest/latest_title_metrics.csv",
                "description": "Features de títulos para análisis de packaging y copy.",
                "recommended_default_sort": "views_delta desc",
                "recommended_use": "Vista de títulos y análisis de señales semánticas.",
            },
            "latest_video_scores": {
                "path": "data/analytics/latest/latest_video_scores.csv",
                "description": "Scores robustos por video (alpha, oportunidad y anomalía).",
                "recommended_default_sort": "alpha_score desc",
                "recommended_use": "Vista de priorización por score para decisiones rápidas.",
            },
            "latest_video_advanced_metrics": {
                "path": "data/analytics/latest/latest_video_advanced_metrics.csv",
                "description": "Métricas avanzadas por video con éxito por horizonte y confianza.",
                "recommended_default_sort": "overall_success_score desc",
                "recommended_use": "Vista avanzada de performance y diagnóstico por video.",
            },
            "latest_channel_advanced_metrics": {
                "path": "data/analytics/latest/latest_channel_advanced_metrics.csv",
                "description": "Métricas avanzadas por canal con momentum y consistencia.",
                "recommended_default_sort": "channel_momentum_score desc",
                "recommended_use": "Vista avanzada de salud de canal y estabilidad.",
            },
            "latest_metric_eligibility": {
                "path": "data/analytics/latest/latest_metric_eligibility.csv",
                "description": "Elegibilidad de métricas por horizonte temporal por video.",
                "recommended_default_sort": "metric_confidence_score asc",
                "recommended_use": "Vista de data quality y filtros de confiabilidad.",
            },
            "channel_baselines": {
                "path": "data/analytics/baselines/channel_baselines.csv",
                "description": "Baselines de desempeño por canal para comparaciones relativas.",
                "recommended_default_sort": "channel_momentum_score desc",
                "recommended_use": "Vista de baseline y benchmarking entre canales.",
            },
            "video_lifecycle_metrics": {
                "path": "data/analytics/baselines/video_lifecycle_metrics.csv",
                "description": "Métricas de ciclo de vida por video según edad y velocidad.",
                "recommended_default_sort": "views_per_day_since_upload desc",
                "recommended_use": "Vista de lifecycle y detección de evergreen.",
            },
            "period_daily_video_metrics": {
                "path": "data/analytics/periods/grain=daily/video_metrics.csv",
                "description": "Agregación diaria por video para análisis de tendencia.",
                "recommended_default_sort": "period_views_delta desc",
                "recommended_use": "Vista temporal de videos en grano diario.",
            },
            "period_weekly_video_metrics": {
                "path": "data/analytics/periods/grain=weekly/video_metrics.csv",
                "description": "Agregación semanal por video para análisis intersemanal.",
                "recommended_default_sort": "period_views_delta desc",
                "recommended_use": "Vista temporal de videos en grano semanal.",
            },
            "period_monthly_video_metrics": {
                "path": "data/analytics/periods/grain=monthly/video_metrics.csv",
                "description": "Agregación mensual por video para evolución macro.",
                "recommended_default_sort": "period_views_delta desc",
                "recommended_use": "Vista temporal de videos en grano mensual.",
            },
            "period_daily_channel_metrics": {
                "path": "data/analytics/periods/grain=daily/channel_metrics.csv",
                "description": "Agregación diaria por canal para monitoreo operativo.",
                "recommended_default_sort": "period_views_delta desc",
                "recommended_use": "Vista temporal de canales en grano diario.",
            },
            "period_weekly_channel_metrics": {
                "path": "data/analytics/periods/grain=weekly/channel_metrics.csv",
                "description": "Agregación semanal por canal para tendencias de mediano plazo.",
                "recommended_default_sort": "period_views_delta desc",
                "recommended_use": "Vista temporal de canales en grano semanal.",
            },
            "period_monthly_channel_metrics": {
                "path": "data/analytics/periods/grain=monthly/channel_metrics.csv",
                "description": "Agregación mensual por canal para lectura ejecutiva.",
                "recommended_default_sort": "period_views_delta desc",
                "recommended_use": "Vista temporal de canales en grano mensual.",
            },
        },
        "recommended_dashboard_views": [
            "overview",
            "videos",
            "channels",
            "scores",
            "advanced_video_metrics",
            "advanced_channel_metrics",
            "titles",
            "lifecycle",
            "periods",
            "data_quality",
        ],
        "recommended_view_default_sorts": {
            "overview": "total_views_delta desc",
            "videos": "views_delta desc",
            "scores": "alpha_score desc",
            "advanced_video_metrics": "overall_success_score desc",
            "advanced_channel_metrics": "channel_momentum_score desc",
            "titles": "views_delta desc",
            "lifecycle": "views_per_day_since_upload desc",
            "periods": "period_views_delta desc",
            "data_quality": "metric_confidence_score asc",
        },
        "dashboard_kpis": [
            "videos_total",
            "channels_total",
            "total_views_delta",
            "total_likes_delta",
            "total_comments_delta",
            "avg_engagement_rate",
            "top_alpha_video_id",
            "top_alpha_video_title",
            "top_channel_by_growth",
            "low_confidence_rows",
        ],
    }

    future_found = [rel for rel in FUTURE_FEATURE_INPUTS if (data_root / rel).exists()]

    manifest_outputs = [
        "analytics/latest/latest_video_metrics.csv",
        "analytics/latest/latest_channel_metrics.csv",
        "analytics/latest/latest_title_metrics.csv",
        "analytics/latest/latest_video_scores.csv",
        "analytics/latest/latest_video_advanced_metrics.csv",
        "analytics/latest/latest_channel_advanced_metrics.csv",
        "analytics/latest/latest_metric_eligibility.csv",
        "analytics/baselines/channel_baselines.csv",
        "analytics/baselines/video_lifecycle_metrics.csv",
        "analytics/periods/grain=daily/video_metrics.csv",
        "analytics/periods/grain=weekly/video_metrics.csv",
        "analytics/periods/grain=monthly/video_metrics.csv",
        "analytics/periods/grain=daily/channel_metrics.csv",
        "analytics/periods/grain=weekly/channel_metrics.csv",
        "analytics/periods/grain=monthly/channel_metrics.csv",
        "analytics/latest/latest_run_metrics.json",
        "analytics/latest/dashboard_index.json",
        "analytics/latest/analytics_manifest.json",
    ]
    row_counts = {
        "analytics/latest/latest_video_metrics.csv": len(video_rows),
        "analytics/latest/latest_channel_metrics.csv": len(channel_rows),
        "analytics/latest/latest_title_metrics.csv": len(title_rows),
        "analytics/latest/latest_video_scores.csv": len(video_scores_rows),
        "analytics/latest/latest_video_advanced_metrics.csv": len(video_advanced_rows),
        "analytics/latest/latest_channel_advanced_metrics.csv": len(channel_advanced_rows),
        "analytics/latest/latest_metric_eligibility.csv": len(eligibility_rows),
        "analytics/baselines/channel_baselines.csv": len(channel_baselines_rows),
        "analytics/baselines/video_lifecycle_metrics.csv": len(video_lifecycle_rows),
        "analytics/periods/grain=daily/video_metrics.csv": len(period_outputs["daily"]["video_rows"]),
        "analytics/periods/grain=weekly/video_metrics.csv": len(period_outputs["weekly"]["video_rows"]),
        "analytics/periods/grain=monthly/video_metrics.csv": len(period_outputs["monthly"]["video_rows"]),
        "analytics/periods/grain=daily/channel_metrics.csv": len(period_outputs["daily"]["channel_rows"]),
        "analytics/periods/grain=weekly/channel_metrics.csv": len(period_outputs["weekly"]["channel_rows"]),
        "analytics/periods/grain=monthly/channel_metrics.csv": len(period_outputs["monthly"]["channel_rows"]),
        "analytics/latest/latest_run_metrics.json": 1,
        "analytics/latest/dashboard_index.json": 1,
        "analytics/latest/analytics_manifest.json": 1,
    }

    analytics_manifest = {
        "generated_at": _now_iso(),
        "source_export_dir": str(source_export_dir),
        "outputs": manifest_outputs,
        "row_counts": row_counts,
        "schema_version": "analytics_v1",
        "dashboard_ready": True,
        "dashboard_index_path": "data/analytics/latest/dashboard_index.json",
        "dashboard_recommended_entrypoint": "data/analytics/latest/dashboard_index.json",
        "scoring_version": "scoring_v1",
        "advanced_metrics_version": "advanced_metrics_v1",
        "success_horizons": {
            "short_term": "0-3 days",
            "mid_term": "4-30 days",
            "long_term": "31-180 days",
        },
        "standardization_methods": [
            "percentile_rank",
            "log1p",
            "robust_z_mad",
            "channel_relative_baseline",
            "format_relative_baseline",
        ],
        "anomaly_method": "robust_z",
        "isolation_forest_ready": False,
        "isolation_forest_reason": "requires larger historical sample and optional sklearn dependency",
        "channel_momentum_note": "temporarily equals channel_growth_percentile; evolve to 7d_vs_30d when period history grows",
        "future_feature_inputs_checked": FUTURE_FEATURE_INPUTS,
        "future_feature_inputs_found": future_found,
        "warnings": warnings,
    }

    analytics_latest.mkdir(parents=True, exist_ok=True)

    output_video = analytics_latest / "latest_video_metrics.csv"
    output_channel = analytics_latest / "latest_channel_metrics.csv"
    output_title = analytics_latest / "latest_title_metrics.csv"
    output_video_scores = analytics_latest / "latest_video_scores.csv"
    output_video_advanced = analytics_latest / "latest_video_advanced_metrics.csv"
    output_channel_advanced = analytics_latest / "latest_channel_advanced_metrics.csv"
    output_metric_eligibility = analytics_latest / "latest_metric_eligibility.csv"
    output_baselines = analytics_root / "baselines" / "channel_baselines.csv"
    output_lifecycle = analytics_root / "baselines" / "video_lifecycle_metrics.csv"
    period_daily_video = analytics_root / "periods" / "grain=daily" / "video_metrics.csv"
    period_weekly_video = analytics_root / "periods" / "grain=weekly" / "video_metrics.csv"
    period_monthly_video = analytics_root / "periods" / "grain=monthly" / "video_metrics.csv"
    period_daily_channel = analytics_root / "periods" / "grain=daily" / "channel_metrics.csv"
    period_weekly_channel = analytics_root / "periods" / "grain=weekly" / "channel_metrics.csv"
    period_monthly_channel = analytics_root / "periods" / "grain=monthly" / "channel_metrics.csv"
    output_run = analytics_latest / "latest_run_metrics.json"
    output_dashboard_index = analytics_latest / "dashboard_index.json"
    output_manifest = analytics_latest / "analytics_manifest.json"

    _write_csv(output_video, VIDEO_METRICS_COLUMNS, video_rows)
    _write_csv(output_channel, CHANNEL_METRICS_COLUMNS, channel_rows)
    _write_csv(output_title, TITLE_METRICS_COLUMNS, title_rows)
    _write_csv(output_video_scores, VIDEO_SCORES_COLUMNS, video_scores_rows)
    _write_csv(output_video_advanced, VIDEO_ADVANCED_COLUMNS, video_advanced_rows)
    _write_csv(output_channel_advanced, CHANNEL_ADVANCED_COLUMNS, channel_advanced_rows)
    _write_csv(output_metric_eligibility, METRIC_ELIGIBILITY_COLUMNS, eligibility_rows)
    _write_csv(output_baselines, CHANNEL_BASELINES_COLUMNS, channel_baselines_rows)
    _write_csv(output_lifecycle, VIDEO_LIFECYCLE_COLUMNS, video_lifecycle_rows)
    _write_csv(period_daily_video, PERIOD_VIDEO_COLUMNS, period_outputs["daily"]["video_rows"])
    _write_csv(period_weekly_video, PERIOD_VIDEO_COLUMNS, period_outputs["weekly"]["video_rows"])
    _write_csv(period_monthly_video, PERIOD_VIDEO_COLUMNS, period_outputs["monthly"]["video_rows"])
    _write_csv(period_daily_channel, PERIOD_CHANNEL_COLUMNS, period_outputs["daily"]["channel_rows"])
    _write_csv(period_weekly_channel, PERIOD_CHANNEL_COLUMNS, period_outputs["weekly"]["channel_rows"])
    _write_csv(period_monthly_channel, PERIOD_CHANNEL_COLUMNS, period_outputs["monthly"]["channel_rows"])
    output_run.write_text(json.dumps(run_metrics, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    output_dashboard_index.write_text(json.dumps(dashboard_index, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    output_manifest.write_text(json.dumps(analytics_manifest, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")

    analytics_root_abs = analytics_root.resolve()
    for path in [
        output_video,
        output_channel,
        output_title,
        output_video_scores,
        output_video_advanced,
        output_channel_advanced,
        output_metric_eligibility,
        output_baselines,
        output_lifecycle,
        period_daily_video,
        period_weekly_video,
        period_monthly_video,
        period_daily_channel,
        period_weekly_channel,
        period_monthly_channel,
        output_run,
        output_dashboard_index,
        output_manifest,
    ]:
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
        "latest_video_scores_csv": _safe_rel(output_video_scores, data_root),
        "latest_video_advanced_metrics_csv": _safe_rel(output_video_advanced, data_root),
        "latest_channel_advanced_metrics_csv": _safe_rel(output_channel_advanced, data_root),
        "latest_metric_eligibility_csv": _safe_rel(output_metric_eligibility, data_root),
        "channel_baselines_csv": _safe_rel(output_baselines, data_root),
        "video_lifecycle_metrics_csv": _safe_rel(output_lifecycle, data_root),
        "daily_video_metrics_csv": _safe_rel(period_daily_video, data_root),
        "weekly_video_metrics_csv": _safe_rel(period_weekly_video, data_root),
        "monthly_video_metrics_csv": _safe_rel(period_monthly_video, data_root),
        "daily_channel_metrics_csv": _safe_rel(period_daily_channel, data_root),
        "weekly_channel_metrics_csv": _safe_rel(period_weekly_channel, data_root),
        "monthly_channel_metrics_csv": _safe_rel(period_monthly_channel, data_root),
        "latest_run_metrics_json": _safe_rel(output_run, data_root),
        "dashboard_index_json": _safe_rel(output_dashboard_index, data_root),
        "analytics_manifest_json": _safe_rel(output_manifest, data_root),
    }
    return result
