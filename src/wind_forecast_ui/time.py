from datetime import UTC, datetime, timedelta
from zoneinfo import ZoneInfo


def next_day_start_end(tz: str = "CET") -> tuple[datetime, datetime]:
    """Get the start and end of the next day in UTC."""
    today = datetime.now(ZoneInfo(tz)).replace(hour=0, minute=0, second=0, microsecond=0)
    start = today.astimezone(UTC) + timedelta(days=1)
    end = start + timedelta(days=1)
    return start, end
