"""
Trading Calendar
Determine if a given date is a US stock market trading day
All dates use US Eastern Time (ET UTC-04:00)
"""

from datetime import date, datetime
from typing import Set
from .timezone_utils import get_et_today


# US Federal Holidays (when stock market is closed)
def get_us_federal_holidays(year: int) -> Set[date]:
    """
    Get US federal holidays for a given year (when stock market is closed)
    
    Args:
        year: Year
        
    Returns:
        Set of holiday dates
    """
    from datetime import timedelta
    
    holidays = set()
    
    # New Year's Day (January 1)
    holidays.add(date(year, 1, 1))
    
    # Martin Luther King Jr. Day (Third Monday in January)
    holidays.add(_get_nth_weekday(year, 1, 0, 3))
    
    # Presidents' Day (Third Monday in February)
    holidays.add(_get_nth_weekday(year, 2, 0, 3))
    
    # Good Friday (Friday before Easter - complex calculation)
    holidays.add(_get_good_friday(year))
    
    # Memorial Day (Last Monday in May)
    holidays.add(_get_last_weekday(year, 5, 0))
    
    # Juneteenth (June 19)
    holidays.add(date(year, 6, 19))
    
    # Independence Day (July 4)
    holidays.add(date(year, 7, 4))
    
    # Labor Day (First Monday in September)
    holidays.add(_get_nth_weekday(year, 9, 0, 1))
    
    # Thanksgiving (Fourth Thursday in November)
    holidays.add(_get_nth_weekday(year, 11, 3, 4))
    
    # Christmas (December 25)
    holidays.add(date(year, 12, 25))
    
    # Adjust holidays that fall on weekends
    adjusted_holidays = set()
    for holiday in holidays:
        if holiday.weekday() == 5:  # Saturday -> observe on Friday
            adjusted_holidays.add(holiday - timedelta(days=1))
        elif holiday.weekday() == 6:  # Sunday -> observe on Monday
            adjusted_holidays.add(holiday + timedelta(days=1))
        else:
            adjusted_holidays.add(holiday)
    
    return adjusted_holidays


def _get_nth_weekday(year: int, month: int, weekday: int, n: int) -> date:
    """
    Get the nth occurrence of a weekday in a given month
    
    Args:
        year: Year
        month: Month (1-12)
        weekday: Day of week (0=Monday, 6=Sunday)
        n: Which occurrence (1=first, 2=second, etc.)
        
    Returns:
        Date of the nth weekday
    """
    # Start from the first day of the month
    first_day = date(year, month, 1)
    first_weekday = first_day.weekday()
    
    # Calculate offset to reach the desired weekday
    offset = (weekday - first_weekday) % 7
    
    # Calculate the date of the nth occurrence
    day = 1 + offset + (n - 1) * 7
    
    return date(year, month, day)


def _get_last_weekday(year: int, month: int, weekday: int) -> date:
    """
    Get the last occurrence of a weekday in a given month
    
    Args:
        year: Year
        month: Month (1-12)
        weekday: Day of week (0=Monday, 6=Sunday)
        
    Returns:
        Date of the last weekday
    """
    from datetime import timedelta
    
    # Start from the first day of next month, then go back one day
    if month == 12:
        next_month = date(year + 1, 1, 1)
    else:
        next_month = date(year, month + 1, 1)
    
    # Last day of current month
    last_day = next_month - timedelta(days=1)
    last_weekday = last_day.weekday()
    
    # Calculate offset backwards to reach the desired weekday
    offset = (last_weekday - weekday) % 7
    
    # Return the last occurrence of the target weekday
    return last_day - timedelta(days=offset)


def _get_good_friday(year: int) -> date:
    """
    Calculate Good Friday (Friday before Easter)
    Using Meeus/Jones/Butcher algorithm
    
    Args:
        year: Year
        
    Returns:
        Date of Good Friday
    """
    from datetime import timedelta
    
    a = year % 19
    b = year // 100
    c = year % 100
    d = b // 4
    e = b % 4
    f = (b + 8) // 25
    g = (b - f + 1) // 3
    h = (19 * a + b - d - g + 15) % 30
    i = c // 4
    k = c % 4
    l = (32 + 2 * e + 2 * i - h - k) % 7
    m = (a + 11 * h + 22 * l) // 451
    month = (h + l - 7 * m + 114) // 31
    day = ((h + l - 7 * m + 114) % 31) + 1
    
    # Easter Sunday
    easter = date(year, month, day)
    
    # Good Friday is 2 days before Easter
    good_friday = easter - timedelta(days=2)
    
    return good_friday


def is_trading_day(check_date: date) -> bool:
    """
    Check if a given date is a US stock market trading day
    
    Args:
        check_date: Date to check
        
    Returns:
        True if trading day, False otherwise
    """
    # Check if weekend (Saturday=5, Sunday=6)
    if check_date.weekday() >= 5:
        return False
    
    # Check if federal holiday
    holidays = get_us_federal_holidays(check_date.year)
    if check_date in holidays:
        return False
    
    return True


def get_next_trading_day(from_date: date) -> date:
    """
    Get the next trading day after a given date
    
    Args:
        from_date: Starting date
        
    Returns:
        Next trading day
    """
    from datetime import timedelta
    
    next_day = from_date + timedelta(days=1)
    
    while not is_trading_day(next_day):
        next_day = next_day + timedelta(days=1)
    
    return next_day


def get_previous_trading_day(from_date: date) -> date:
    """
    Get the previous trading day before a given date
    
    Args:
        from_date: Starting date
        
    Returns:
        Previous trading day
    """
    from datetime import timedelta
    
    prev_day = from_date - timedelta(days=1)
    
    while not is_trading_day(prev_day):
        prev_day = prev_day - timedelta(days=1)
    
    return prev_day


def count_trading_days(start_date: date, end_date: date) -> int:
    """
    Count the number of trading days between two dates (inclusive)
    
    Args:
        start_date: Start date
        end_date: End date
        
    Returns:
        Number of trading days
    """
    from datetime import timedelta
    
    count = 0
    current = start_date
    
    while current <= end_date:
        if is_trading_day(current):
            count += 1
        current = current + timedelta(days=1)

    return count


def is_trading_day_et() -> bool:
    """
    Check if today (in ET timezone) is a US stock market trading day

    Returns:
        True if today is a trading day, False otherwise
    """
    return is_trading_day(get_et_today())