"""
Data Masker
===========
Deterministic, one-way masking for PII fields returned by MCP tools.

Why this exists
---------------
The MCP server returns query results to Claude (Anthropic infrastructure).
Even though the Olist dataset uses pseudonymous IDs rather than real names,
customer_unique_id is a stable cross-dataset linkage key — under GDPR Article 4(1)
it qualifies as personal data because it *can* identify a natural person when
combined with other data.

Masking approach
----------------
HMAC-SHA256 with a server-side secret key:

    masked = "cust_" + hmac_sha256(secret, raw_id)[:8]

Plain SHA256 without a key is vulnerable to rainbow-table reversal if the
input space is known (e.g. all 32-char hex strings). HMAC requires knowledge
of the secret key to reproduce, making offline reversal infeasible.

Properties:
  ✓ Deterministic  — same input always produces same mask (supports joins/filters)
  ✓ One-way        — cannot recover original ID without the secret key
  ✓ Collision-safe — 32-bit output over ~96K customers gives ~0% collision rate
  ✗ Not encryption — do not treat masked IDs as recoverable

Secret key
----------
Set MASKING_SECRET env var before starting the server:

    export MASKING_SECRET="$(openssl rand -hex 32)"
    python mcp/server.py

In development the module falls back to a fixed dev key and logs a warning.
Never use the dev key in production — anyone with the key can reproduce masks.

Limitations
-----------
- Masking protects linkage, not inference. Aggregate fields (total_spend_brl,
  avg_review_score) are kept unmasked because they carry no identity signal on
  their own. If future tools return individual transaction amounts or granular
  location data, those fields should be added to the mask config.
- Masked IDs are still pseudonyms, not anonymous data under GDPR. The server
  operator retains the secret key and can theoretically reverse the mapping.
  True anonymisation requires destroying the key.
- This module masks at the API boundary (what Claude sees). It does not affect
  what is stored in DuckDB.
"""

import hashlib
import hmac
import logging
import os
import re
import sys
from functools import wraps
from typing import Any, Callable

logger = logging.getLogger(__name__)

# ── Secret key ────────────────────────────────────────────────────────────────

_ENV_KEY = os.getenv("MASKING_SECRET", "")

if _ENV_KEY:
    _SECRET = _ENV_KEY.encode()
else:
    _SECRET = b"cx-analytics-dev-do-not-use-in-production"
    logger.warning(
        "MASKING_SECRET not set — using insecure dev key. "
        "Set the env var before running in production."
    )


# ── Core hash primitive ───────────────────────────────────────────────────────

def _hmac8(value: str) -> str:
    """Return the first 8 hex characters of HMAC-SHA256(secret, value).

    8 hex chars = 4 bytes = 32 bits. Over the ~96K unique customer IDs in this
    dataset the expected collision count is < 0.001 (birthday bound).
    """
    digest = hmac.new(_SECRET, value.encode(), hashlib.sha256).hexdigest()
    return digest[:8]


# ── Masking functions ─────────────────────────────────────────────────────────

def mask_customer_id(customer_id: str) -> str:
    """Mask a customer unique ID to a deterministic pseudonym.

    Input:  "861eff4711a542e4b93843c6dd7febb0"   (32-char hex)
    Output: "cust_3a9f1b2c"                       (prefix + 8-char HMAC)

    The "cust_" prefix makes masked IDs visually distinct from raw IDs in
    logs or API responses, preventing accidental mixing.
    """
    if not customer_id:
        return customer_id
    return f"cust_{_hmac8(str(customer_id))}"


def mask_email(email: str) -> str:
    """Mask an email address, keeping the first character and domain TLD.

    Input:  "john.doe@example.com"
    Output: "j***@***.com"

    Not present in the Olist dataset — implemented for future extensibility.
    """
    if not email or "@" not in email:
        return "***"
    local, _, domain = email.partition("@")
    masked_local = local[0] + "***" if local else "***"
    parts = domain.rsplit(".", 1)
    masked_domain = "***."+parts[1] if len(parts) == 2 else "***"
    return f"{masked_local}@{masked_domain}"


def mask_phone(phone: str) -> str:
    """Mask a phone number, keeping the country code prefix.

    Input:  "+1-555-0123"   →  "+1-***-****"
    Input:  "555-867-5309"  →  "***-***-****"

    Not present in the Olist dataset — implemented for future extensibility.
    """
    if not phone:
        return "***"
    # Replace digit sequences after the first token with asterisks.
    tokens = re.split(r"([\s\-\.\(\)])", phone.strip())
    result, found_first = [], False
    for token in tokens:
        if re.match(r"^\+?\d+$", token):
            if not found_first:
                result.append(token)   # keep country code / first block
                found_first = True
            else:
                result.append("*" * len(token))
        else:
            result.append(token)
    return "".join(result)


def mask_name(name: str) -> str:
    """Mask a personal name, keeping only the first character of each word.

    Input:  "John Smith"   →  "J*** S***"
    Input:  "Maria da Silva"  →  "M*** d*** S***"

    Not present in the Olist dataset — implemented for future extensibility.
    """
    if not name:
        return "***"
    return " ".join(
        (word[0] + "***" if word else "***")
        for word in name.split()
    )


# ── Column mask registry ──────────────────────────────────────────────────────

#: Maps column name → masking function.
#: Columns absent from this dict are returned unmasked.
#:
#: Sensitivity rationale per column:
#:   customer_unique_id  MASK   — stable linkage key, qualifies as personal data
#:   state               KEEP   — geographic aggregate, low re-identification risk
#:   city                KEEP   — geographic aggregate, low re-identification risk
#:   total_spend_brl     KEEP   — aggregate metric, no identity signal alone
#:   avg_review_score    KEEP   — aggregate metric
#:   first/last_order_at KEEP   — dates without identity context
#:   churn_probability   KEEP   — model output, no identity signal alone
COLUMN_MASKS: dict[str, Callable[[Any], Any]] = {
    "customer_unique_id": mask_customer_id,
    # Extend here as new tools introduce PII columns:
    # "email":  mask_email,
    # "phone":  mask_phone,
    # "name":   mask_name,
}


# ── Decorator ─────────────────────────────────────────────────────────────────

def mask_pii(
    columns: dict[str, Callable[[Any], Any]] | None = None,
) -> Callable:
    """Decorator that masks PII columns in a tool's list[dict] return value.

    Usage:
        @mcp.tool()
        @mask_pii()                        # uses COLUMN_MASKS defaults
        def get_customer_segments(...): ...

        @mcp.tool()
        @mask_pii({"email": mask_email})   # override for this tool only
        def get_contacts(...): ...

    Masking is applied after the function returns — tool signatures and
    FastMCP schema generation are unaffected.

    Notes:
      - Customer IDs are masked for privacy
      - Masking is deterministic (same ID always masks to same value)
      - Suitable for Anthropic data retention policies: masked IDs cannot be
        linked back to individuals without the server-side MASKING_SECRET
    """
    mask_map = columns if columns is not None else COLUMN_MASKS

    def decorator(func: Callable) -> Callable:
        @wraps(func)            # preserves __name__, __doc__, __annotations__
        def wrapper(*args: Any, **kwargs: Any) -> Any:
            rows = func(*args, **kwargs)
            if not isinstance(rows, list):
                return rows     # passthrough for non-list returns

            return [
                {
                    col: (mask_map[col](val) if col in mask_map and val is not None else val)
                    for col, val in row.items()
                }
                for row in rows
            ]
        return wrapper

    return decorator
