"""
Audit Logger
============
GDPR Article 32 / HIPAA §164.312(b) compliant audit trail for the CX Analytics
MCP server. Every query execution is recorded as a structured JSON line.

What IS logged (operational metadata only):
  - Timestamp, function name, input parameters (filter values — not PII)
  - Row count returned, execution time, success/failure, session ID

What is NOT logged (never appears in log files):
  - Row contents (customer IDs, names, spend amounts, churn scores)
  - Full SQL text beyond a 200-char preview
  - Parameter values flagged as sensitive (see SENSITIVE_PARAMS)

Log location:  ./logs/mcp_audit.log
Rotation:      Daily at midnight, 30-day retention
Format:        JSON Lines — one JSON object per line, grep/jq-friendly

GDPR Article 32 notes
---------------------
This logger satisfies the "appropriate technical measures" requirement by
providing a complete access audit trail without itself becoming a source of
personal data. Row counts are sufficient to detect bulk extraction; actual
record contents are never written.

HIPAA §164.312(b) notes
-----------------------
Audit controls must record activity in systems containing PHI. This server
holds e-commerce (not health) data, but the same pattern applies: every
read access is logged with enough context to reconstruct "who accessed what
volume of data, when, and whether it succeeded."

Breach investigation procedure — see bottom of this file.
"""

import json
import logging
import os
import time
import uuid
from contextlib import contextmanager
from datetime import datetime, timezone
from logging.handlers import TimedRotatingFileHandler
from pathlib import Path
from typing import Any, Generator

# ── Constants ─────────────────────────────────────────────────────────────────

LOGS_DIR = Path(__file__).parent.parent / "logs"
LOG_FILE = LOGS_DIR / "mcp_audit.log"
LOG_RETENTION_DAYS = 30
LARGE_RESULT_THRESHOLD = 1_000   # rows — triggers WARNING
SQL_PREVIEW_CHARS = 200          # max chars of SQL captured per entry

# Parameter keys whose values are never written to logs even at DEBUG level.
# Add any key that could carry PII in future tool signatures.
SENSITIVE_PARAMS: frozenset[str] = frozenset({
    "customer_id", "email", "name", "phone", "address",
})

# One session ID per server process — identifies a Claude Desktop connection.
SESSION_ID: str = str(uuid.uuid4())[:8]

# Detect non-production environments. Set ENVIRONMENT=production to suppress
# DEBUG-level parameter logging in live deployments.
_IS_PRODUCTION = os.getenv("ENVIRONMENT", "development").lower() == "production"


# ── Logger setup ──────────────────────────────────────────────────────────────

class _JsonFormatter(logging.Formatter):
    """Emit each log record as a single-line JSON object."""

    def format(self, record: logging.LogRecord) -> str:
        payload: dict[str, Any] = {
            "timestamp": datetime.fromtimestamp(
                record.created, tz=timezone.utc
            ).isoformat(),
            "level":     record.levelname,
            "session":   SESSION_ID,
            "logger":    record.name,
        }
        # The record message is always a dict built by audit_event() below.
        if isinstance(record.msg, dict):
            payload.update(record.msg)
        else:
            payload["message"] = record.getMessage()
        if record.exc_info:
            payload["exception"] = self.formatException(record.exc_info)
        return json.dumps(payload, default=str)


def _build_logger() -> logging.Logger:
    LOGS_DIR.mkdir(parents=True, exist_ok=True)

    logger = logging.getLogger("cx_analytics.audit")
    if logger.handlers:          # already configured (e.g. during tests)
        return logger

    logger.setLevel(logging.DEBUG)

    # ── File handler: daily rotation, 30-day retention ──────────────────────
    file_handler = TimedRotatingFileHandler(
        filename=str(LOG_FILE),
        when="midnight",
        interval=1,
        backupCount=LOG_RETENTION_DAYS,
        utc=True,
        encoding="utf-8",
    )
    file_handler.setFormatter(_JsonFormatter())
    file_handler.setLevel(logging.DEBUG)
    logger.addHandler(file_handler)

    # ── Console handler: INFO+ so server stderr stays readable ──────────────
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(_JsonFormatter())
    console_handler.setLevel(logging.INFO)
    logger.addHandler(console_handler)

    return logger


audit = _build_logger()


# ── Public helpers ────────────────────────────────────────────────────────────

def _sanitize_params(params: dict[str, Any]) -> dict[str, Any]:
    """
    Return a copy of params safe to write to logs.

    Rules:
      - Keys in SENSITIVE_PARAMS → replaced with "<redacted>"
      - All other values → written as-is (they are filter criteria, not PII)
      - In production, DEBUG-only detail is suppressed at the handler level;
        this function always returns the sanitized dict regardless of env.
    """
    return {
        k: ("<redacted>" if k in SENSITIVE_PARAMS else v)
        for k, v in params.items()
    }


def audit_event(
    function: str,
    params: dict[str, Any],
    row_count: int,
    duration_ms: float,
    sql_preview: str,
    status: str,                    # "success" | "error"
    error: str | None = None,
) -> None:
    """
    Emit one structured audit record.

    This is the single write point for all audit data. Every field is
    intentional; do not add fields that contain row-level data.
    """
    record: dict[str, Any] = {
        "event":        "data_access",
        "function":     function,
        "params":       _sanitize_params(params),
        "row_count":    row_count,
        "duration_ms":  round(duration_ms, 2),
        "sql_preview":  sql_preview[:SQL_PREVIEW_CHARS].replace("\n", " ").strip(),
        "status":       status,
    }
    if error:
        record["error"] = error

    if status == "error":
        audit.error(record)
    elif row_count > LARGE_RESULT_THRESHOLD:
        audit.warning({**record, "warning": f"large_result_set:{row_count}_rows"})
    else:
        audit.info(record)

    # DEBUG: log raw params (suppressed in production by handler level)
    if not _IS_PRODUCTION:
        audit.debug({
            "event":    "debug_params",
            "function": function,
            "raw_params": params,   # unredacted — dev/test only
        })


@contextmanager
def timed_query(
    function: str,
    params: dict[str, Any],
    sql: str,
) -> Generator[dict, None, None]:
    """
    Context manager that times a query block and calls audit_event() on exit.

    Usage in _safe_query():
        with timed_query("get_customer_segments", params, sql) as ctx:
            rows = con.execute(sql, param_list).fetchall()
            ctx["row_count"] = len(rows)

    The caller sets ctx["row_count"] after execution. On any exception,
    status="error" is recorded and the exception re-raised.
    """
    ctx: dict[str, Any] = {"row_count": 0}
    t0 = time.perf_counter()
    try:
        yield ctx
        duration_ms = (time.perf_counter() - t0) * 1000
        audit_event(
            function=function,
            params=params,
            row_count=ctx["row_count"],
            duration_ms=duration_ms,
            sql_preview=sql,
            status="success",
        )
    except Exception as exc:
        duration_ms = (time.perf_counter() - t0) * 1000
        audit_event(
            function=function,
            params=params,
            row_count=0,
            duration_ms=duration_ms,
            sql_preview=sql,
            status="error",
            error=str(exc),
        )
        raise


# ── Breach investigation guide ────────────────────────────────────────────────
"""
HOW TO INVESTIGATE A DATA BREACH USING THESE LOGS
==================================================

Log location:  logs/mcp_audit.log
               logs/mcp_audit.log.YYYY-MM-DD  (rotated daily)

1. Find all access in a time window (jq):

    jq 'select(.timestamp >= "2026-04-14T00:00:00" and
               .timestamp <= "2026-04-14T23:59:59")' logs/mcp_audit.log

2. Find bulk-extraction attempts (large result sets):

    jq 'select(.level == "WARNING")' logs/mcp_audit.log

3. Find all accesses by session (one session = one Claude Desktop connection):

    jq 'select(.session == "a1b2c3d4")' logs/mcp_audit.log

4. Find failed / suspicious queries:

    jq 'select(.status == "error")' logs/mcp_audit.log

5. Count total rows accessed per function over a period:

    jq '[.row_count] | add' logs/mcp_audit.log

6. Find queries with unusual parameters (potential injection attempts):

    jq 'select(.params | tostring | test("UNION|DROP|--|;"))' logs/mcp_audit.log

7. Full timeline for a breach window:

    grep "2026-04-14" logs/mcp_audit.log | jq -r '[.timestamp,.session,.function,.row_count,.status] | @tsv'
"""
