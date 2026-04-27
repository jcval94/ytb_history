"""Pipeline orchestrator for daily run and dry-run estimation."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

from ytb_history.clients.quota_meter import QuotaMeter
from ytb_history.clients.youtube_client import YouTubeClient
from ytb_history.config import load_settings
from ytb_history.domain.models import RunSummary
from ytb_history.repositories.channel_registry_repo import ChannelRegistryRepo
from ytb_history.repositories.delta_repo import DeltaRepo
from ytb_history.repositories.run_report_repo import RunReportRepo
from ytb_history.repositories.snapshot_repo import SnapshotRepo
from ytb_history.repositories.video_catalog_repo import VideoCatalogRepo
from ytb_history.services.discovery_service import discover_recent_videos
from ytb_history.services.enrichment_service import fetch_video_snapshots
from ytb_history.services.quota_service import build_quota_report, estimate_total_quota_cost
from ytb_history.services.resolver_service import normalize_channel_url, resolve_channels
from ytb_history.services.snapshot_service import persist_snapshot_and_deltas
from ytb_history.services.tracking_service import build_tracking_video_ids, update_tracking_catalog




def _load_default_channel_urls() -> list[str]:
    channels_path = Path(__file__).resolve().parents[2] / "config" / "channels.py"
    namespace: dict[str, Any] = {}
    exec(channels_path.read_text(encoding="utf-8"), namespace)
    values = namespace.get("CHANNEL_URLS", [])
    if not isinstance(values, list):
        raise ValueError(f"CHANNEL_URLS must be a list in {channels_path}")
    return [str(item) for item in values]

def _as_utc(execution_date: datetime | None) -> datetime:
    if execution_date is None:
        return datetime.now(timezone.utc)
    if execution_date.tzinfo is None:
        return execution_date.replace(tzinfo=timezone.utc)
    return execution_date.astimezone(timezone.utc)


def _extract_quota_meter(youtube_client: Any) -> QuotaMeter:
    meter = getattr(youtube_client, "_quota_meter", None) or getattr(youtube_client, "quota_meter", None)
    if meter is None:
        raise ValueError("youtube_client must expose a QuotaMeter via _quota_meter or quota_meter")
    return meter


def _build_channel_errors(channel_records: list[Any], discovery_errors: list[Any]) -> list[dict[str, Any]]:
    errors: list[dict[str, Any]] = []
    for channel in channel_records:
        if channel.error_message:
            errors.append(
                {
                    "stage": "resolver",
                    "channel_url": channel.channel_url,
                    "channel_id": channel.channel_id,
                    "channel_name": channel.channel_name,
                    "error": channel.error_message,
                }
            )
    for report in discovery_errors:
        if report.error_message:
            errors.append(
                {
                    "stage": "discovery",
                    "channel_id": report.channel_id,
                    "channel_name": report.channel_name,
                    "uploads_playlist_id": report.uploads_playlist_id,
                    "error": report.error_message,
                }
            )
    return errors


def _estimate_uncached_channels(channel_urls: list[str], existing_registry: list[Any]) -> int:
    cached_urls = {
        normalize_channel_url(record.channel_url)
        for record in existing_registry
        if record.resolver_status == "ok" and record.channel_url
    }
    unique_input = {normalize_channel_url(url) for url in channel_urls}
    return len([url for url in unique_input if url not in cached_urls])


def run_pipeline(
    *,
    execution_date: datetime | None = None,
    channel_urls: list[str] | None = None,
    settings_path: str | Path = "config/settings.yaml",
    data_dir: str | Path = "data",
    youtube_client: YouTubeClient | None = None,
) -> dict[str, Any]:
    """Execute the complete daily pipeline."""
    execution_date_utc = _as_utc(execution_date)
    settings = load_settings(settings_path)
    urls = list(channel_urls or _load_default_channel_urls())

    data_root = Path(data_dir)
    registry_repo = ChannelRegistryRepo(data_root / "state" / "channel_registry.jsonl")
    video_catalog_repo = VideoCatalogRepo(data_root / "state" / "tracked_videos_catalog.jsonl")
    snapshot_repo = SnapshotRepo(base_dir=data_root / "snapshots")
    delta_repo = DeltaRepo(base_dir=data_root / "deltas")
    run_report_repo = RunReportRepo(base_dir=data_root / "reports")

    existing_registry = registry_repo.load()
    quota_meter = QuotaMeter()
    client = youtube_client or YouTubeClient(quota_meter=quota_meter)
    if youtube_client is not None:
        quota_meter = _extract_quota_meter(youtube_client)

    channels = resolve_channels(urls, youtube_client=client, channel_registry_repo=registry_repo)

    channels_total = len(urls)
    channels_ok_records = [item for item in channels if item.resolver_status == "ok"]
    channels_failed_records = [item for item in channels if item.resolver_status != "ok"]

    discovery_result = discover_recent_videos(
        channels,
        since_datetime=execution_date_utc - timedelta(days=settings["discovery_window_days"]),
        youtube_client=client,
        quota_meter=quota_meter,
        max_pages_per_channel=settings["max_pages_per_channel"],
    )

    catalog = video_catalog_repo.load()
    tracking_video_ids = build_tracking_video_ids(
        catalog,
        discovery_result.recent_video_ids,
        execution_date=execution_date_utc,
    )

    unique_ok_uploads = {
        record.uploads_playlist_id
        for record in channels_ok_records
        if record.uploads_playlist_id
    }
    estimated_units = estimate_total_quota_cost(
        uncached_channels=_estimate_uncached_channels(urls, existing_registry),
        channels_to_check=len(unique_ok_uploads),
        pages_per_channel=settings["max_pages_per_channel"],
        videos_to_track=len(tracking_video_ids),
        batch_size=settings["youtube_batch_size"],
    )
    pre_quota_report = build_quota_report(
        execution_date=execution_date_utc,
        estimated_units=estimated_units,
        observed_units=quota_meter.as_dict(),
        operational_limit=settings["operational_quota_limit"],
        warning_limit=settings["warning_quota_limit"],
        soft_warning_limit=settings["soft_warning_quota_limit"],
    )

    errors = _build_channel_errors(channels, discovery_result.channel_reports)
    discovery_report_path = run_report_repo.save_discovery_report(execution_date_utc, discovery_result.channel_reports)
    channel_errors_path = run_report_repo.save_channel_errors(execution_date_utc, errors)

    if pre_quota_report.should_abort:
        quota_report_path = run_report_repo.save_quota_report(execution_date_utc, pre_quota_report)
        summary = RunSummary(
            execution_date=execution_date_utc,
            status="aborted_quota_guardrail",
            channels_total=channels_total,
            channels_ok=len(channels_ok_records),
            channels_failed=len(channels_failed_records),
            videos_discovered=len(discovery_result.recent_video_ids),
            videos_tracked=len(tracking_video_ids),
            quota_status=pre_quota_report.limit_status,
            estimated_quota_units=pre_quota_report.total_estimated_units,
            observed_quota_units=pre_quota_report.total_observed_units,
            errors=[item["error"] for item in errors],
        )
        run_summary_path = run_report_repo.save_run_summary(execution_date_utc, summary)
        result = summary.to_dict()
        result.update(
            {
                "quota_report_path": str(quota_report_path),
                "run_summary_path": str(run_summary_path),
                "discovery_report_path": str(discovery_report_path),
                "channel_errors_path": str(channel_errors_path),
            }
        )
        return result

    enrichment = fetch_video_snapshots(
        tracking_video_ids,
        youtube_client=client,
        execution_date=execution_date_utc,
        batch_size=settings["youtube_batch_size"],
    )

    updated_catalog = update_tracking_catalog(
        catalog,
        enrichment.snapshots,
        execution_date=execution_date_utc,
        tracking_window_days=settings["tracking_window_days"],
    )
    video_catalog_repo.save(updated_catalog)

    persistence = persist_snapshot_and_deltas(
        execution_date=execution_date_utc,
        snapshots=enrichment.snapshots,
        snapshot_repo=snapshot_repo,
        delta_repo=delta_repo,
    )

    final_quota_report = build_quota_report(
        execution_date=execution_date_utc,
        estimated_units=estimated_units,
        observed_units=quota_meter.as_dict(),
        operational_limit=settings["operational_quota_limit"],
        warning_limit=settings["warning_quota_limit"],
        soft_warning_limit=settings["soft_warning_quota_limit"],
    )

    quota_report_path = run_report_repo.save_quota_report(execution_date_utc, final_quota_report)

    all_errors = list(errors)
    all_errors.extend({"stage": "enrichment", "error": msg} for msg in enrichment.errors)
    all_errors.extend(
        {"stage": "enrichment", "video_id": video_id, "error": "video_unavailable"}
        for video_id in enrichment.unavailable_video_ids
    )

    status = "success"
    if all_errors or channels_failed_records or enrichment.unavailable_video_ids:
        status = "success_with_warnings"

    summary = RunSummary(
        execution_date=execution_date_utc,
        status=status,
        channels_total=channels_total,
        channels_ok=len(channels_ok_records),
        channels_failed=len(channels_failed_records),
        videos_discovered=len(discovery_result.recent_video_ids),
        videos_tracked=len(tracking_video_ids),
        videos_snapshotted=len(enrichment.snapshots),
        videos_unavailable=len(enrichment.unavailable_video_ids),
        snapshot_path=persistence.snapshot_path,
        delta_path=persistence.delta_path,
        quota_status=final_quota_report.limit_status,
        estimated_quota_units=final_quota_report.total_estimated_units,
        observed_quota_units=final_quota_report.total_observed_units,
        errors=[item["error"] for item in all_errors],
    )
    run_summary_path = run_report_repo.save_run_summary(execution_date_utc, summary)
    # Keep channel errors report persisted with enrichment warnings included.
    channel_errors_path = run_report_repo.save_channel_errors(execution_date_utc, all_errors)

    result = summary.to_dict()
    result.update(
        {
            "quota_report_path": str(quota_report_path),
            "run_summary_path": str(run_summary_path),
            "discovery_report_path": str(discovery_report_path),
            "channel_errors_path": str(channel_errors_path),
        }
    )
    return result


def run_dry_run(
    *,
    execution_date: datetime | None = None,
    channel_urls: list[str] | None = None,
    settings_path: str | Path = "config/settings.yaml",
    data_dir: str | Path = "data",
) -> dict[str, Any]:
    """Estimate quota and guardrail status without API calls or writes."""
    execution_date_utc = _as_utc(execution_date)
    settings = load_settings(settings_path)
    urls = list(channel_urls or _load_default_channel_urls())

    catalog_repo = VideoCatalogRepo(Path(data_dir) / "state" / "tracked_videos_catalog.jsonl")
    catalog = catalog_repo.load()

    estimated_units = estimate_total_quota_cost(
        uncached_channels=0,
        channels_to_check=len({normalize_channel_url(url) for url in urls}),
        pages_per_channel=settings["max_pages_per_channel"],
        videos_to_track=len(catalog),
        batch_size=settings["youtube_batch_size"],
    )
    quota_report = build_quota_report(
        execution_date=execution_date_utc,
        estimated_units=estimated_units,
        observed_units={},
        operational_limit=settings["operational_quota_limit"],
        warning_limit=settings["warning_quota_limit"],
        soft_warning_limit=settings["soft_warning_quota_limit"],
    )
    return quota_report.to_dict()
