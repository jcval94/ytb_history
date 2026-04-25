from ytb_history.services.quota_service import evaluate_quota_status


def test_quota_status_ok() -> None:
    assert evaluate_quota_status(
        total_estimated_units=100,
        operational_limit=7000,
        warning_limit=5000,
        soft_warning_limit=1000,
    ) == "ok"


def test_quota_status_soft_warning() -> None:
    assert evaluate_quota_status(
        total_estimated_units=1200,
        operational_limit=7000,
        warning_limit=5000,
        soft_warning_limit=1000,
    ) == "soft_warning"


def test_quota_status_warning() -> None:
    assert evaluate_quota_status(
        total_estimated_units=5500,
        operational_limit=7000,
        warning_limit=5000,
        soft_warning_limit=1000,
    ) == "warning"


def test_quota_status_over_limit() -> None:
    assert evaluate_quota_status(
        total_estimated_units=8000,
        operational_limit=7000,
        warning_limit=5000,
        soft_warning_limit=1000,
    ) == "over_operational_limit"
