"""
Tests for data_masker.py
Run: python -m pytest mcp/tests/test_data_masker.py -v
"""

import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).parent.parent))
from data_masker import (
    mask_customer_id,
    mask_email,
    mask_name,
    mask_phone,
    mask_pii,
    COLUMN_MASKS,
)


# ── mask_customer_id ──────────────────────────────────────────────────────────

class TestMaskCustomerId:
    def test_output_format(self):
        result = mask_customer_id("861eff4711a542e4b93843c6dd7febb0")
        assert result.startswith("cust_")
        assert len(result) == len("cust_") + 8

    def test_deterministic(self):
        raw = "861eff4711a542e4b93843c6dd7febb0"
        assert mask_customer_id(raw) == mask_customer_id(raw)

    def test_different_inputs_differ(self):
        a = mask_customer_id("861eff4711a542e4b93843c6dd7febb0")
        b = mask_customer_id("290c77bc529b7ac935b93aa66c333dc3")
        assert a != b

    def test_masked_id_not_equal_to_raw(self):
        raw = "861eff4711a542e4b93843c6dd7febb0"
        assert mask_customer_id(raw) != raw

    def test_raw_id_not_in_masked_output(self):
        raw = "861eff4711a542e4b93843c6dd7febb0"
        assert raw not in mask_customer_id(raw)

    def test_empty_string_passthrough(self):
        assert mask_customer_id("") == ""

    def test_none_passthrough(self):
        # mask_pii decorator guards against None, but function itself is safe
        assert mask_customer_id(None) is None  # type: ignore[arg-type]


# ── mask_email ────────────────────────────────────────────────────────────────

class TestMaskEmail:
    def test_basic(self):
        result = mask_email("john@example.com")
        assert result == "j***@***.com"

    def test_keeps_first_char_only(self):
        result = mask_email("alice@domain.org")
        assert result.startswith("a***@")

    def test_tld_preserved(self):
        assert mask_email("user@test.io").endswith(".io")
        assert mask_email("user@test.co.uk").endswith(".uk")

    def test_no_at_sign_returns_redacted(self):
        assert mask_email("notanemail") == "***"

    def test_empty_returns_redacted(self):
        assert mask_email("") == "***"

    def test_original_not_in_result(self):
        email = "john.doe@company.com"
        result = mask_email(email)
        assert "john.doe" not in result
        assert "company" not in result


# ── mask_phone ────────────────────────────────────────────────────────────────

class TestMaskPhone:
    def test_keeps_country_code(self):
        result = mask_phone("+1-555-0123")
        assert result.startswith("+1")

    def test_masks_middle_digits(self):
        result = mask_phone("+1-555-0123")
        assert "555" not in result
        assert "0123" not in result

    def test_asterisks_replace_digits(self):
        result = mask_phone("+1-555-0123")
        assert "***" in result or "****" in result

    def test_empty_returns_redacted(self):
        assert mask_phone("") == "***"


# ── mask_name ─────────────────────────────────────────────────────────────────

class TestMaskName:
    def test_single_name(self):
        assert mask_name("John") == "J***"

    def test_full_name(self):
        result = mask_name("John Smith")
        assert result == "J*** S***"

    def test_three_part_name(self):
        result = mask_name("Maria da Silva")
        assert result == "M*** d*** S***"

    def test_original_not_in_result(self):
        name = "Robert Johnson"
        result = mask_name(name)
        assert "Robert" not in result
        assert "Johnson" not in result

    def test_empty_returns_redacted(self):
        assert mask_name("") == "***"


# ── mask_pii decorator ────────────────────────────────────────────────────────

class TestMaskPiiDecorator:
    def _make_rows(self):
        return [
            {
                "customer_unique_id": "861eff4711a542e4b93843c6dd7febb0",
                "state":              "SP",
                "city":               "sao paulo",
                "total_spend_brl":    350.00,
                "avg_review_score":   4.2,
            },
            {
                "customer_unique_id": "290c77bc529b7ac935b93aa66c333dc3",
                "state":              "RJ",
                "city":               "rio de janeiro",
                "total_spend_brl":    120.50,
                "avg_review_score":   3.8,
            },
        ]

    def test_customer_id_masked(self):
        @mask_pii()
        def tool():
            return self._make_rows()

        rows = tool()
        for row in rows:
            assert row["customer_unique_id"].startswith("cust_")
            assert len(row["customer_unique_id"]) == 13

    def test_non_pii_columns_unchanged(self):
        @mask_pii()
        def tool():
            return self._make_rows()

        rows = tool()
        assert rows[0]["state"] == "SP"
        assert rows[0]["city"] == "sao paulo"
        assert rows[0]["total_spend_brl"] == 350.00
        assert rows[0]["avg_review_score"] == 4.2

    def test_deterministic_across_calls(self):
        @mask_pii()
        def tool():
            return self._make_rows()

        first  = tool()[0]["customer_unique_id"]
        second = tool()[0]["customer_unique_id"]
        assert first == second

    def test_different_ids_get_different_masks(self):
        @mask_pii()
        def tool():
            return self._make_rows()

        rows = tool()
        assert rows[0]["customer_unique_id"] != rows[1]["customer_unique_id"]

    def test_none_values_skipped(self):
        @mask_pii()
        def tool():
            return [{"customer_unique_id": None, "state": "SP"}]

        rows = tool()
        assert rows[0]["customer_unique_id"] is None   # not crashed or mutated

    def test_empty_list_passthrough(self):
        @mask_pii()
        def tool():
            return []

        assert tool() == []

    def test_preserves_function_name(self):
        @mask_pii()
        def get_customer_segments():
            """Original docstring."""
            return []

        assert get_customer_segments.__name__ == "get_customer_segments"
        assert get_customer_segments.__doc__ == "Original docstring."

    def test_custom_column_map(self):
        @mask_pii({"city": lambda v: "***"})
        def tool():
            return [{"customer_unique_id": "abc", "city": "sao paulo"}]

        rows = tool()
        assert rows[0]["city"] == "***"
        assert rows[0]["customer_unique_id"] == "abc"   # not in custom map

    def test_non_list_return_passthrough(self):
        @mask_pii()
        def tool():
            return {"status": "ok"}   # dict, not list

        assert tool() == {"status": "ok"}


# ── Before / after example ────────────────────────────────────────────────────

class TestBeforeAfterExample:
    """Documents the transformation visible to Anthropic's infrastructure."""

    def test_before_masking(self):
        raw = {
            "customer_unique_id": "861eff4711a542e4b93843c6dd7febb0",
            "state": "SP",
            "total_spend_brl": 350.0,
        }
        # Before: real pseudonymous ID visible
        assert raw["customer_unique_id"] == "861eff4711a542e4b93843c6dd7febb0"

    def test_after_masking(self):
        @mask_pii()
        def tool():
            return [{
                "customer_unique_id": "861eff4711a542e4b93843c6dd7febb0",
                "state": "SP",
                "total_spend_brl": 350.0,
            }]

        row = tool()[0]
        # After: only opaque masked ID visible to Claude
        assert row["customer_unique_id"].startswith("cust_")
        assert "861eff" not in row["customer_unique_id"]
        assert row["state"] == "SP"
        assert row["total_spend_brl"] == 350.0
