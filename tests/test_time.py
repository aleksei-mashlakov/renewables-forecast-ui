from datetime import UTC, datetime

from src.wind_forecast_ui.time import last_day_start_end, next_day_start_end


def test_last_day_start_end() -> None:
    """Test the last_day_start_end function."""
    start, end = last_day_start_end("CET")
    assert start < end
    assert start < datetime.now(UTC)
    assert end < datetime.now(UTC)
    assert (end - start).days == 1


def test_next_day_start_end_utc() -> None:
    """Test the next_day_start_end function with UTC timezone."""
    start, end = next_day_start_end("CET")
    assert start < end
    assert start > datetime.now(UTC)
    assert end > datetime.now(UTC)
    assert (end - start).days == 1
