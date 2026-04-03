from datetime import datetime, timedelta
import pytz


def get_period_bounds(period_type: str = "MONTHLY", tz_name: str = "UTC",
                      start_day: int = 1) -> tuple[datetime, datetime]:
    tz = pytz.timezone(tz_name)
    now = datetime.now(tz)

    if period_type == "MONTHLY":
        period_start = now.replace(day=start_day, hour=0, minute=0, second=0, microsecond=0)
        if now < period_start:
            month = period_start.month - 1 or 12
            year = period_start.year if period_start.month > 1 else period_start.year - 1
            period_start = period_start.replace(year=year, month=month)
        if period_start.month == 12:
            period_end = period_start.replace(year=period_start.year + 1, month=1)
        else:
            period_end = period_start.replace(month=period_start.month + 1)

    elif period_type == "WEEKLY":
        days_since = (now.weekday() - (start_day % 7)) % 7
        period_start = (now - timedelta(days=days_since)).replace(
            hour=0, minute=0, second=0, microsecond=0
        )
        period_end = period_start + timedelta(days=7)

    elif period_type == "QUARTERLY":
        q = (now.month - 1) // 3
        period_start = now.replace(month=q * 3 + 1, day=1,
                                   hour=0, minute=0, second=0, microsecond=0)
        next_q = q + 1
        if next_q >= 4:
            period_end = period_start.replace(year=period_start.year + 1, month=1)
        else:
            period_end = period_start.replace(month=next_q * 3 + 1)
    else:
        period_start = now.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
        if now.month == 12:
            period_end = period_start.replace(year=period_start.year + 1, month=1)
        else:
            period_end = period_start.replace(month=period_start.month + 1)

    return period_start, period_end


def format_period(period_start: datetime, period_end: datetime) -> str:
    return f"{period_start.strftime('%b %d, %Y')} – {period_end.strftime('%b %d, %Y')}"
