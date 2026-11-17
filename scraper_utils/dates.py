"""Date helpers for parsers: whatever a site prints -> ISO text, or None."""
import re
from datetime import datetime, timezone

from scraper_utils.text import clean as text
from scraper_utils.values import to_int

_LONG_DATE_FORMATS = ("%B %d, %Y", "%b %d, %Y", "%d %B %Y", "%d %b %Y", "%Y-%m-%d", "%m/%d/%Y", "%d/%m/%Y")


def iso_date(value):
    """'21 Sep 2026' / '2026-09-22T…' / 'September 22, 2026' -> 'YYYY-MM-DD'.
    A slash date is read month first ('03/04/2026' is 4 March)."""
    value = text(value)
    if not value:
        return None
    match = re.match(r"(\d{4})-(\d{2})-(\d{2})", value)
    if match:
        return "-".join(match.groups())
    for fmt in _LONG_DATE_FORMATS:
        try:
            return datetime.strptime(value, fmt).strftime("%Y-%m-%d")
        except ValueError:
            continue
    return None


def iso_datetime(value):
    """'2026-09-18T04:30:49.433Z' / {dateTimeUtc} -> '2026-09-18T04:30:49Z' (the
    clock time as written, to the second); a bare date -> 'YYYY-MM-DD'."""
    if isinstance(value, dict):
        value = value.get("dateTimeUtc") or value.get("dateTime") or value.get("shortAbsoluteLabel")
    value = text(value)
    if not value:
        return None
    match = re.match(r"(\d{4}-\d{2}-\d{2})T(\d{2}:\d{2}:\d{2})", value)
    if match:
        return f"{match.group(1)}T{match.group(2)}Z"
    return iso_date(value)


def iso_day(value):
    """'2026-04-07' / '2026-04-07T...' -> '2026-04-07' (the calendar date as
    written); anything that does not start with an ISO date -> None."""
    if not isinstance(value, str):
        return None
    match = re.match(r"(\d{4}-\d{2}-\d{2})", value.strip())
    return match.group(1) if match else None


def iso_utc(value):
    """'2026-10-05T20:47:24.561+11:00' -> '2026-10-05T09:47:24Z'. A time with
    no offset is taken as UTC already."""
    if not value or not isinstance(value, str):
        return None
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def epoch_utc(epoch):
    """Epoch seconds -> '2026-09-17T14:27:44Z'; 0, junk and out-of-range -> None."""
    seconds = to_int(epoch)
    if not seconds:
        return None
    try:
        return datetime.fromtimestamp(seconds, tz=timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    except (OverflowError, OSError, ValueError):
        return None
