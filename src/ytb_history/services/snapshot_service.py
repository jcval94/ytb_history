"""Snapshot persistence service."""

from __future__ import annotations

from datetime import datetime

from ytb_history.domain.models import SnapshotPersistenceResult, VideoSnapshot
from ytb_history.repositories.delta_repo import DeltaRepo
from ytb_history.repositories.snapshot_repo import SnapshotRepo
from ytb_history.services.delta_service import build_deltas


def persist_snapshot_and_deltas(
    *,
    execution_date: datetime,
    snapshots: list[VideoSnapshot],
    snapshot_repo: SnapshotRepo,
    delta_repo: DeltaRepo,
) -> SnapshotPersistenceResult:
    previous_snapshots = snapshot_repo.load_latest_before(execution_date)
    deltas = build_deltas(current=snapshots, previous=previous_snapshots)

    snapshot_path = snapshot_repo.save_for_run(execution_date, snapshots)
    delta_path = delta_repo.save_for_run(execution_date, deltas)

    return SnapshotPersistenceResult(
        snapshot_path=str(snapshot_path),
        delta_path=str(delta_path),
        snapshots_written=len(snapshots),
        deltas_written=len(deltas),
        previous_snapshot_found=bool(previous_snapshots),
    )
