"""
Timezone Utilities
Centralized timezone handling for US Eastern Time (ET UTC-04:00)
"""

from datetime import datetime, date, timezone, timedelta


# US Eastern Time (ET) - Fixed offset UTC-04:00
# Note: This uses a fixed offset. For DST-aware handling, use pytz or zoneinfo
ET_OFFSET = timezone(timedelta(hours=-4))


def get_et_now() -> datetime:
    """
    Get current datetime in US Eastern Time (UTC-04:00)

    Returns:
        Current datetime in ET timezone
    """
    return datetime.now(ET_OFFSET)


def get_et_today() -> date:
    """
    Get current date in US Eastern Time (UTC-04:00)

    Returns:
        Current date in ET timezone
    """
    return get_et_now().date()


def utc_to_et(utc_dt: datetime) -> datetime:
    """
    Convert UTC datetime to US Eastern Time

    Args:
        utc_dt: UTC datetime (naive or aware)

    Returns:
        Datetime in ET timezone
    """
    if utc_dt.tzinfo is None:
        # Assume naive datetime is UTC
        utc_dt = utc_dt.replace(tzinfo=timezone.utc)
    return utc_dt.astimezone(ET_OFFSET)


def et_to_utc(et_dt: datetime) -> datetime:
    """
    Convert ET datetime to UTC

    Args:
        et_dt: ET datetime (naive or aware)

    Returns:
        Datetime in UTC timezone
    """
    if et_dt.tzinfo is None:
        # Assume naive datetime is ET
        et_dt = et_dt.replace(tzinfo=ET_OFFSET)
    return et_dt.astimezone(timezone.utc)


def get_et_timestamp_iso() -> str:
    """
    Get current ET timestamp in ISO format with timezone indicator

    Returns:
        ISO format string like "2025-01-15T10:30:00-04:00"
    """
    return get_et_now().isoformat()
