"""Channel registry repository."""

from __future__ import annotations

from dataclasses import replace
from datetime import datetime
from pathlib import Path

from ytb_history.domain.models import ChannelRecord
from ytb_history.storage.jsonl import read_jsonl, write_jsonl

DEFAULT_CHANNEL_REGISTRY_PATH = Path("data/state/channel_registry.jsonl")


class ChannelRegistryRepo:
    def __init__(self, path: str | Path = DEFAULT_CHANNEL_REGISTRY_PATH) -> None:
        self._path = Path(path)

    def load(self) -> list[ChannelRecord]:
        rows = read_jsonl(self._path)
        records: list[ChannelRecord] = []
        for row in rows:
            resolved_at = datetime.fromisoformat(row["resolved_at"])
            records.append(
                ChannelRecord(
                    channel_url=row.get("channel_url", ""),
                    channel_id=row.get("channel_id", ""),
                    channel_name=row.get("channel_name", ""),
                    uploads_playlist_id=row.get("uploads_playlist_id", ""),
                    resolved_at=resolved_at,
                    resolver_status=row.get("resolver_status", "ok"),
                    error_message=row.get("error_message"),
                )
            )
        return records

    def save(self, records: list[ChannelRecord]) -> None:
        deduped = self._deduplicate(records)
        write_jsonl(self._path, [record.to_dict() for record in deduped])

    def upsert(self, records: list[ChannelRecord]) -> list[ChannelRecord]:
        merged = self.load() + records
        deduped = self._deduplicate(merged)
        self.save(deduped)
        return deduped

    @staticmethod
    def _deduplicate(records: list[ChannelRecord]) -> list[ChannelRecord]:
        latest_ok_by_channel_id: dict[str, ChannelRecord] = {}
        passthrough_errors: list[ChannelRecord] = []

        for record in records:
            if record.resolver_status != "ok":
                passthrough_errors.append(record)
                continue

            channel_id = record.channel_id.strip()
            if not channel_id:
                passthrough_errors.append(replace(record, resolver_status="error", error_message="Missing channel_id"))
                continue

            current = latest_ok_by_channel_id.get(channel_id)
            if current is None or record.resolved_at >= current.resolved_at:
                latest_ok_by_channel_id[channel_id] = record

        return sorted(
            [*latest_ok_by_channel_id.values(), *passthrough_errors],
            key=lambda item: item.resolved_at,
        )


def load_channel_registry(path: str | Path = DEFAULT_CHANNEL_REGISTRY_PATH) -> list[ChannelRecord]:
    return ChannelRegistryRepo(path).load()
