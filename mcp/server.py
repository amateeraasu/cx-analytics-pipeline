"""
CX Analytics MCP Server
=======================
Connects Claude to the CX Analytics DuckDB marts via the Model Context Protocol.

Run:
    python mcp/server.py

Then add to Claude Desktop's config (~/Library/Application Support/Claude/claude_desktop_config.json):
    {
      "mcpServers": {
        "cx-analytics": {
          "command": "python",
          "args": ["/absolute/path/to/cx-analytics-pipeline/mcp/server.py"]
        }
      }
    }

Example questions Claude can answer once connected:
  - "What was the CSAT rate trend over 2017?"
  - "Which states have the worst on-time delivery rates?"
  - "Show me the top 10 customers by spend who are at high churn risk."
  - "How does average review score differ between one-time and repeat customers?"
"""

import duckdb
from datetime import date
from pathlib import Path
from mcp.server.fastmcp import FastMCP
from audit_logger import timed_query

# ── Path to the compiled DuckDB file ────────────────────────────────────────
REPO_ROOT = Path(__file__).parent.parent
DB_PATH = REPO_ROOT / "data" / "cx_analytics.duckdb"
SCHEMA = "main_customer_experience"

mcp = FastMCP(
    name="cx-analytics",
    instructions=(
        "Query the CX Analytics Platform — 100K+ e-commerce orders, "
        "customer satisfaction KPIs, delivery performance, and churn predictions. "
        "All data is from the Olist Brazilian E-Commerce dataset (2016–2018)."
    ),
)


def _connect() -> duckdb.DuckDBPyConnection:
    """Return a read-only connection to the compiled DuckDB file."""
    if not DB_PATH.exists():
        raise FileNotFoundError(
            f"DuckDB file not found at {DB_PATH}. "
            "Run `dbt run --profiles-dir .` from the repo root first."
        )
    return duckdb.connect(str(DB_PATH), read_only=True)


# Dataset boundaries — dates outside this range return no rows anyway,
# and accepting arbitrary future dates widens the attack surface needlessly.
_DATASET_MIN = date(2016, 1, 1)
_DATASET_MAX = date(2018, 12, 31)


def date_validator(
    value: str,
    name: str,
    *,
    min_date: date = _DATASET_MIN,
    max_date: date = _DATASET_MAX,
) -> date:
    """Parse and validate an ISO date string. Return a datetime.date object.

    Defences layered in order:
      1. datetime.date.fromisoformat() rejects every non-YYYY-MM-DD string,
         including injection payloads, slashes, and impossible calendar dates.
         A regex like r"^\\d{4}-\\d{2}-\\d{2}$" passes "2016-02-31"; fromisoformat
         raises ValueError for it.
      2. Boundary check rejects dates outside the dataset window so callers
         cannot probe for data that does not exist.
      3. Error messages name the parameter but never expose internal path,
         schema names, or implementation details.

    Args:
        value:    Candidate date string from the caller.
        name:     Parameter name — included in ValueError for caller context.
        min_date: Earliest accepted date (default: dataset start 2016-01-01).
        max_date: Latest accepted date (default: dataset end 2018-12-31).

    Returns:
        datetime.date — safe to pass directly as a DuckDB ? parameter.

    Raises:
        ValueError: on invalid format, impossible calendar date, or out-of-range.

    Blocked examples:
        "01/01/2016"              → ValueError (wrong format)
        "2016-02-31"              → ValueError (impossible date)
        "2016-01-01' OR '1'='1"  → ValueError (fromisoformat rejects on first
                                    non-digit after position 10)
        "2020-01-01"              → ValueError (outside dataset range)
        "2017-06-15"              → date(2017, 6, 15)  ✓
    """
    if not isinstance(value, str):
        raise ValueError(f"'{name}' must be a string.")

    try:
        parsed = date.fromisoformat(value)
    except ValueError:
        # Don't echo the value back — it may contain injection payload.
        raise ValueError(
            f"'{name}' is not a valid date. Expected YYYY-MM-DD."
        )

    if not (min_date <= parsed <= max_date):
        raise ValueError(
            f"'{name}' is outside the accepted range "
            f"({min_date} to {max_date})."
        )

    return parsed


def _safe_query(
    sql: str,
    params: list | None = None,
    *,
    _caller: str = "run_sql",
    _audit_params: dict | None = None,
) -> list[dict]:
    """Execute a parameterised read-only SQL query, audit every execution.

    Args:
        sql:           Query string with ? placeholders for user values.
        params:        Values bound to ? placeholders in order.
        _caller:       Tool function name — written to the audit log.
        _audit_params: Tool-level input parameters written to the audit log.
                       These are filter criteria (not row data).
    """
    sql_upper = sql.strip().upper()
    if not sql_upper.startswith("SELECT") and not sql_upper.startswith("WITH"):
        raise ValueError("Only SELECT / WITH queries are allowed.")

    with timed_query(_caller, _audit_params or {}, sql) as ctx:
        with _connect() as con:
            rel = con.execute(sql, params or [])
            cols = [d[0] for d in rel.description]
            rows = [dict(zip(cols, row)) for row in rel.fetchall()]
        ctx["row_count"] = len(rows)

    return rows


def _require_iso_date(value: str, name: str) -> str:
    """Raise ValueError if value is not YYYY-MM-DD. Returns value unchanged."""
    if not _ISO_DATE_RE.match(value):
        raise ValueError(
            f"'{name}' must be an ISO date (YYYY-MM-DD), got: {value!r}"
        )
    return value


# ── Tool 1: Run arbitrary SQL ────────────────────────────────────────────────

@mcp.tool()
def run_sql(query: str) -> list[dict]:
    """
    Run any read-only SQL query against the CX Analytics DuckDB database.

    Available tables (use schema prefix 'main_customer_experience.'):
      - fct_orders              — one row per order; delivery, payment, review fields
      - dim_customers           — one row per unique customer; lifetime metrics + segments
      - cx_satisfaction_summary — monthly KPI rollup (CSAT, on-time rate, GMV)
      - mart_churn_predictions  — churn probability + risk tier per customer

    Example:
      SELECT state, round(avg(avg_days_to_deliver), 2) AS avg_delivery_days
      FROM main_customer_experience.dim_customers
      GROUP BY state
      ORDER BY avg_delivery_days DESC
      LIMIT 10
    """
    return _safe_query(query, _caller="run_sql", _audit_params={"query_preview": query[:80]})


# ── Tool 2: Monthly KPIs ─────────────────────────────────────────────────────

@mcp.tool()
def get_monthly_kpis(
    start_month: str = "2016-01-01",
    end_month: str = "2018-12-31",
) -> list[dict]:
    """
    Return monthly CX KPIs: CSAT rate, on-time delivery rate, avg review score,
    total orders, and GMV (BRL).

    Args:
        start_month: ISO date string, YYYY-MM-DD. Accepted range: 2016-01-01 to 2018-12-31.
        end_month:   ISO date string, YYYY-MM-DD. Must be >= start_month.

    Returns one row per month within the range, ordered chronologically.
    """
    start = date_validator(start_month, "start_month")
    end   = date_validator(end_month,   "end_month")

    if start > end:
        raise ValueError("'start_month' must not be later than 'end_month'.")

    sql = f"""
        SELECT
            order_month,
            total_orders,
            avg_review_score,
            csat_rate,
            on_time_rate,
            avg_days_to_deliver,
            avg_order_value_brl,
            total_gmv_brl,
            low_score_orders,
            voucher_orders
        FROM {SCHEMA}.cx_satisfaction_summary
        WHERE order_month BETWEEN ? AND ?
        ORDER BY order_month
    """
    # Pass date objects directly — DuckDB binds them as DATE without string parsing.
    return _safe_query(
        sql, [start, end],
        _caller="get_monthly_kpis",
        _audit_params={"start_month": str(start), "end_month": str(end)},
    )


# ── Tool 3: Customer segments ────────────────────────────────────────────────

@mcp.tool()
def get_customer_segments(
    state: str | None = None,
    order_frequency_segment: str | None = None,
    satisfaction_segment: str | None = None,
    min_total_spend_brl: float = 0,
    limit: int = 50,
) -> list[dict]:
    """
    Query the customer dimension table with optional filters.

    Args:
        state: Brazilian state code, e.g. 'SP', 'RJ', 'MG'. None = all states.
        order_frequency_segment: 'one_time', 'repeat', or 'loyal'. None = all.
        satisfaction_segment: 'satisfied', 'neutral', or 'dissatisfied'. None = all.
        min_total_spend_brl: Minimum lifetime spend in BRL (default 0).
        limit: Max rows to return (default 50, max 500).

    Returns customer records sorted by total spend descending.
    """
    limit = min(limit, 500)

    # Build WHERE clauses and params list together — ? placeholders only,
    # no user input ever touches the SQL string.
    #
    # BLOCKED: state="SP' OR '1'='1"
    #   Before: WHERE state = 'SP' OR '1'='1'   → returns all rows
    #   After:  WHERE state = ?  params=["SP' OR '1'='1"]  → 0 rows (no match)
    #
    # BLOCKED: order_frequency_segment="one_time' UNION SELECT * FROM secrets --"
    #   After:  WHERE order_frequency_segment = ?  → treated as literal string
    filters: list[str] = []
    params: list = []

    if state:
        filters.append("state = ?")
        params.append(state.upper())
    if order_frequency_segment:
        filters.append("order_frequency_segment = ?")
        params.append(order_frequency_segment)
    if satisfaction_segment:
        filters.append("satisfaction_segment = ?")
        params.append(satisfaction_segment)
    if min_total_spend_brl > 0:
        filters.append("total_spend_brl >= ?")
        params.append(float(min_total_spend_brl))

    where = "WHERE " + " AND ".join(filters) if filters else ""

    sql = f"""
        SELECT
            customer_unique_id,
            state,
            city,
            total_orders,
            total_spend_brl,
            avg_order_value_brl,
            avg_review_score,
            avg_days_to_deliver,
            order_frequency_segment,
            satisfaction_segment,
            first_order_at,
            last_order_at,
            customer_lifespan_days
        FROM {SCHEMA}.dim_customers
        {where}
        ORDER BY total_spend_brl DESC
        LIMIT ?
    """
    params.append(limit)
    return _safe_query(
        sql, params,
        _caller="get_customer_segments",
        _audit_params={
            "state": state,
            "order_frequency_segment": order_frequency_segment,
            "satisfaction_segment": satisfaction_segment,
            "min_total_spend_brl": min_total_spend_brl,
            "limit": limit,
        },
    )


# ── Tool 4: Churn risk ───────────────────────────────────────────────────────

@mcp.tool()
def get_churn_risk(
    risk_tier: str | None = None,
    state: str | None = None,
    min_spend_brl: float = 0,
    limit: int = 50,
) -> list[dict]:
    """
    Query churn predictions with customer context.
    Requires mart_churn_predictions (run churn_prediction.ipynb first).

    Args:
        risk_tier: 'critical' (≥0.85), 'high' (≥0.65), 'medium' (≥0.40), 'low'.
                   None = all tiers.
        state: Brazilian state code, e.g. 'SP'. None = all states.
        min_spend_brl: Filter to customers with lifetime spend above this threshold.
        limit: Max rows (default 50, max 500).

    Returns customers sorted by churn probability descending.
    """
    limit = min(limit, 500)

    # BLOCKED: risk_tier="high' UNION SELECT customer_id, password FROM users --"
    #   After:  WHERE churn_risk_tier = ?  → literal string match, UNION never executes
    filters: list[str] = []
    params: list = []

    if risk_tier:
        filters.append("churn_risk_tier = ?")
        params.append(risk_tier)
    if state:
        filters.append("state = ?")
        params.append(state.upper())
    if min_spend_brl > 0:
        filters.append("total_spend_brl >= ?")
        params.append(float(min_spend_brl))

    where = "WHERE " + " AND ".join(filters) if filters else ""

    sql = f"""
        SELECT
            customer_unique_id,
            round(churn_probability, 4)  AS churn_probability,
            churn_risk_tier,
            state,
            order_frequency_segment,
            satisfaction_segment,
            total_orders,
            total_spend_brl,
            avg_review_score,
            last_order_at
        FROM {SCHEMA}.mart_churn_predictions
        {where}
        ORDER BY churn_probability DESC
        LIMIT ?
    """
    params.append(limit)
    return _safe_query(
        sql, params,
        _caller="get_churn_risk",
        _audit_params={
            "risk_tier": risk_tier,
            "state": state,
            "min_spend_brl": min_spend_brl,
            "limit": limit,
        },
    )


# ── Tool 5: Delivery performance by dimension ────────────────────────────────

@mcp.tool()
def get_delivery_performance(
    group_by: str = "state",
    min_orders: int = 100,
    limit: int = 30,
) -> list[dict]:
    """
    Aggregate delivery and satisfaction metrics grouped by a dimension.

    Args:
        group_by: Column to aggregate by. Options:
                  'state'            — geographic breakdown
                  'order_day_of_week' — day-of-week patterns (0=Sun … 6=Sat)
                  'order_month'      — monthly trend
                  'primary_payment_type' — by payment method
        min_orders: Minimum order count to include a group (filters noise).
        limit: Max rows to return (default 30).

    Returns groups sorted by avg_days_to_deliver descending (worst first).
    """
    allowed = {"state", "order_day_of_week", "order_month", "primary_payment_type"}
    if group_by not in allowed:
        raise ValueError(f"group_by must be one of: {allowed}")

    # state lives in dim_customers — needs a join
    if group_by == "state":
        dim_col = "c.state"
        from_clause = f"{SCHEMA}.fct_orders f JOIN {SCHEMA}.dim_customers c USING (customer_sk)"
        where_clause = "f.order_status = 'delivered' AND c.state IS NOT NULL"
    else:
        dim_col = f"f.{group_by}"
        from_clause = f"{SCHEMA}.fct_orders f"
        where_clause = f"f.order_status = 'delivered' AND f.{group_by} IS NOT NULL"

    sql = f"""
        SELECT
            {dim_col}                                                 AS dimension,
            count(*)                                                  AS total_orders,
            round(avg(f.days_to_deliver), 2)                         AS avg_days_to_deliver,
            round(avg(f.delivery_delta_days), 2)                     AS avg_delta_vs_estimate,
            round(
                sum(CASE WHEN f.delivered_on_time THEN 1 ELSE 0 END)::float
                / nullif(count(*), 0) * 100, 1
            )                                                         AS on_time_pct,
            round(avg(f.review_score), 3)                            AS avg_review_score,
            round(
                sum(CASE WHEN f.review_score >= 4 THEN 1 ELSE 0 END)::float
                / nullif(count(f.review_score), 0) * 100, 1
            )                                                         AS csat_pct
        FROM {from_clause}
        WHERE {where_clause}
        GROUP BY {dim_col}
        HAVING count(*) >= ?
        ORDER BY avg_days_to_deliver DESC
        LIMIT ?
    """
    # group_by is safe: validated against an explicit allowlist above.
    # min_orders and limit are user-supplied integers → parameterized.
    return _safe_query(
        sql, [int(min_orders), int(limit)],
        _caller="get_delivery_performance",
        _audit_params={"group_by": group_by, "min_orders": min_orders, "limit": limit},
    )


# ── Tool 6: Schema explorer ──────────────────────────────────────────────────

@mcp.tool()
def list_tables() -> list[dict]:
    """
    List all available tables and their column names.
    Use this to explore the schema before writing a custom SQL query.
    """
    sql = f"""
        SELECT
            table_name,
            string_agg(column_name, ', ' ORDER BY ordinal_position) AS columns
        FROM information_schema.columns
        WHERE table_schema = '{SCHEMA}'
        GROUP BY table_name
        ORDER BY table_name
    """
    return _safe_query(sql, _caller="list_tables", _audit_params={"schema": SCHEMA})


# ── Entry point ──────────────────────────────────────────────────────────────

if __name__ == "__main__":
    mcp.run()
