"""The date helpers: what each variant promises."""
from scraper_utils import dates as D


def test_iso_day_reads_only_an_iso_prefix():
    assert D.iso_day("2026-04-07T17:12:35Z") == "2026-04-07" and D.iso_day(" 2026-04-07 ") == "2026-04-07"
    assert D.iso_day("April 7, 2026") is None and D.iso_day("04/07/2026") is None and D.iso_day(20260407) is None


def test_iso_date_reads_long_and_slash_dates_month_first():
    assert D.iso_date("21 Sep 2026") == "2026-09-21" and D.iso_date("September 22, 2026") == "2026-09-22"
    assert D.iso_date("03/04/2026") == "2026-03-04" and D.iso_date("31/12/2026") == "2026-12-31"
    assert D.iso_date("2026-09-22T10:00:00") == "2026-09-22" and D.iso_date("soon") is None and D.iso_date(None) is None


def test_iso_datetime_keeps_the_clock_as_written():
    assert D.iso_datetime("2026-09-18T04:30:49.433Z") == "2026-09-18T04:30:49Z"
    assert D.iso_datetime("2026-09-18T04:30:49+05:30") == "2026-09-18T04:30:49Z"
    assert D.iso_datetime({"dateTimeUtc": "2026-09-18T04:30:49Z"}) == "2026-09-18T04:30:49Z"
    assert D.iso_datetime("2026-09-18") == "2026-09-18" and D.iso_datetime("") is None


def test_iso_utc_converts_to_utc():
    assert D.iso_utc("2026-10-05T20:47:24.561+11:00") == "2026-10-05T09:47:24Z"
    assert D.iso_utc("2026-10-05T20:47:24Z") == "2026-10-05T20:47:24Z" and D.iso_utc("2026-10-05T20:47:24") == "2026-10-05T20:47:24Z"
    assert D.iso_utc("nope") is None and D.iso_utc(None) is None and D.iso_utc(5) is None


def test_epoch_utc():
    assert D.epoch_utc(1772600767) == "2026-03-04T05:06:07Z" and D.epoch_utc("1772600767") == "2026-03-04T05:06:07Z"
    assert D.epoch_utc(0) is None and D.epoch_utc(None) is None and D.epoch_utc(10 ** 20) is None
