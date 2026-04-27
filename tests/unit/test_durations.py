from ytb_history.utils.durations import parse_youtube_duration_to_seconds


def test_parse_youtube_duration_to_seconds_supported_formats() -> None:
    assert parse_youtube_duration_to_seconds("PT15M33S") == 933
    assert parse_youtube_duration_to_seconds("PT1H2M3S") == 3723
    assert parse_youtube_duration_to_seconds("PT45S") == 45
    assert parse_youtube_duration_to_seconds("PT10M") == 600
    assert parse_youtube_duration_to_seconds("PT2H") == 7200
    assert parse_youtube_duration_to_seconds("P1DT2H") == 93600


def test_parse_youtube_duration_to_seconds_invalid_values() -> None:
    assert parse_youtube_duration_to_seconds(None) == 0
    assert parse_youtube_duration_to_seconds("") == 0
    assert parse_youtube_duration_to_seconds("invalid") == 0
