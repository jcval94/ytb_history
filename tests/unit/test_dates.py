from datetime import datetime, timezone

from ytb_history.utils.dates import days_ago


def test_days_ago_uses_base() -> None:
    base = datetime(2026, 4, 25, tzinfo=timezone.utc)
    assert days_ago(7, base=base) == datetime(2026, 4, 18, tzinfo=timezone.utc)
