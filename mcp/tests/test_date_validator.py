"""
Tests for date_validator()
==========================
Run from the repo root:
    python -m pytest mcp/tests/test_date_validator.py -v
"""

import sys
from datetime import date
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).parent.parent))
from server import date_validator, _DATASET_MIN, _DATASET_MAX


# ── Happy path ────────────────────────────────────────────────────────────────

class TestValidDates:
    def test_returns_date_object(self):
        result = date_validator("2017-06-15", "start_month")
        assert isinstance(result, date)
        assert result == date(2017, 6, 15)

    def test_dataset_lower_boundary(self):
        assert date_validator("2016-01-01", "start_month") == _DATASET_MIN

    def test_dataset_upper_boundary(self):
        assert date_validator("2018-12-31", "end_month") == _DATASET_MAX

    def test_midpoint(self):
        assert date_validator("2017-07-01", "end_month") == date(2017, 7, 1)

    def test_leap_day_in_range(self):
        # 2016 is a leap year and within the dataset window
        assert date_validator("2016-02-29", "start_month") == date(2016, 2, 29)

    def test_custom_min_max(self):
        # Caller can override boundaries for other use cases
        result = date_validator(
            "2020-06-01", "d",
            min_date=date(2020, 1, 1),
            max_date=date(2020, 12, 31),
        )
        assert result == date(2020, 6, 1)


# ── Format errors ─────────────────────────────────────────────────────────────

class TestInvalidFormat:
    """These must raise ValueError — wrong format, not wrong value."""

    @pytest.mark.parametrize("bad_value", [
        "01/01/2016",       # MM/DD/YYYY — common US format, wrong here
        "01-01-2016",       # DD-MM-YYYY
        "2016/01/01",       # YYYY/MM/DD
        "20160101",         # no separators
        "2016-1-1",         # single-digit month/day
        "Jan 1, 2016",      # human-readable
        "2016-13-01",       # month 13 does not exist
        "2016-00-15",       # month 0 does not exist
        "2016-01-00",       # day 0 does not exist
        "2016-01-32",       # day 32 does not exist
        "2016-02-30",       # Feb 30 never exists
        "2016-02-31",       # Feb 31 never exists — regex alone passes this
        "2017-11-31",       # Nov has 30 days
        "",                 # empty string
        "not-a-date",       # obviously wrong
        "2016",             # year only
        "2016-01",          # year-month only
    ])
    def test_bad_format_raises(self, bad_value):
        with pytest.raises(ValueError) as exc_info:
            date_validator(bad_value, "start_month")
        # Error must name the parameter but not echo the bad value back.
        # (Skip the echo check for empty string — it is contained in everything.)
        assert "start_month" in str(exc_info.value)
        if bad_value:
            assert bad_value not in str(exc_info.value)

    def test_non_string_raises(self):
        with pytest.raises(ValueError):
            date_validator(20160101, "start_month")  # type: ignore[arg-type]

    def test_none_raises(self):
        with pytest.raises((ValueError, AttributeError)):
            date_validator(None, "start_month")  # type: ignore[arg-type]


# ── SQL injection attempts ────────────────────────────────────────────────────

class TestInjectionBlocked:
    """
    fromisoformat() is the primary defence: it raises ValueError on the first
    character that isn't part of a valid ISO 8601 date, so injection payloads
    never reach the query layer.
    """

    @pytest.mark.parametrize("payload", [
        "2016-01-01' OR '1'='1",                    # classic string injection
        "2016-01-01' OR '1'='1' --",                # with comment
        "2016-01-01'; DROP TABLE orders; --",        # destructive
        "2016-01-01' UNION SELECT * FROM secrets --",# UNION extract
        "2016-01-01\nOR 1=1",                        # newline bypass attempt
        "2016-01-01 OR 1=1",                         # space after valid date
        "' OR '1'='1",                               # no date prefix
        "2016-01-01%27%20OR%20%271%27%3D%271",      # URL-encoded
    ])
    def test_injection_payload_raises(self, payload):
        with pytest.raises(ValueError):
            date_validator(payload, "start_month")


# ── Out-of-range dates ────────────────────────────────────────────────────────

class TestOutOfRange:
    """Valid ISO dates, but outside the dataset window."""

    @pytest.mark.parametrize("out_of_range", [
        "2015-12-31",   # one day before dataset start
        "2019-01-01",   # one day after dataset end
        "2020-06-15",   # well outside
        "1900-01-01",   # historical
        "2099-12-31",   # far future
    ])
    def test_out_of_range_raises(self, out_of_range):
        with pytest.raises(ValueError) as exc_info:
            date_validator(out_of_range, "end_month")
        assert "end_month" in str(exc_info.value)

    def test_error_message_includes_bounds(self):
        with pytest.raises(ValueError) as exc_info:
            date_validator("2025-01-01", "start_month")
        msg = str(exc_info.value)
        assert "2016-01-01" in msg   # min shown
        assert "2018-12-31" in msg   # max shown

    def test_error_message_does_not_echo_payload(self):
        payload = "2025-01-01"
        with pytest.raises(ValueError) as exc_info:
            date_validator(payload, "start_month")
        # Boundary values are fine to show; the input value itself should not
        # be echoed (it could be an injection string in other callers).
        assert payload not in str(exc_info.value)


# ── Logical ordering ──────────────────────────────────────────────────────────

class TestDateOrdering:
    """start > end should be caught at the call site, not inside date_validator.
    These tests document that date_validator itself does NOT enforce ordering —
    get_monthly_kpis() does that separately."""

    def test_validator_does_not_enforce_order(self):
        # Both are valid individually — ordering is the caller's responsibility
        start = date_validator("2018-01-01", "start_month")
        end   = date_validator("2016-06-01", "end_month")
        assert start > end   # validator accepted both; caller must check


# ── Error message quality ─────────────────────────────────────────────────────

class TestErrorMessages:
    def test_format_error_names_param(self):
        with pytest.raises(ValueError) as exc_info:
            date_validator("bad", "my_param")
        assert "my_param" in str(exc_info.value)

    def test_format_error_does_not_mention_regex(self):
        with pytest.raises(ValueError) as exc_info:
            date_validator("bad", "start_month")
        assert "regex" not in str(exc_info.value).lower()
        assert "pattern" not in str(exc_info.value).lower()

    def test_format_error_does_not_mention_internal_paths(self):
        with pytest.raises(ValueError) as exc_info:
            date_validator("bad", "start_month")
        msg = str(exc_info.value)
        assert "/" not in msg
        assert "server.py" not in msg
        assert "fromisoformat" not in msg
