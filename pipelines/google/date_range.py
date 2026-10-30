import datetime
import re


def reporting_dates(days_back=30, *, start_date=None, end_date=None):
    """Resolve inclusive Google report dates without changing rolling loads."""
    if start_date is None and end_date is None:
        end = datetime.date.today()
        return end - datetime.timedelta(days=days_back), end
    if start_date is None or end_date is None:
        raise ValueError("Provide both start_date and end_date.")
    dates = []
    for name, value in (("start_date", start_date), ("end_date", end_date)):
        if not isinstance(value, str) or not re.fullmatch(r"[0-9]{4}-[0-9]{2}-[0-9]{2}", value):
            raise ValueError(f"{name} must use YYYY-MM-DD.")
        try:
            dates.append(datetime.date.fromisoformat(value))
        except ValueError as exc:
            raise ValueError(f"{name} must be a valid YYYY-MM-DD date.") from exc
    if dates[0] > dates[1]:
        raise ValueError("start_date must be on or before end_date.")
    return tuple(dates)
