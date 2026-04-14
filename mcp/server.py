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

import os
import duckdb
from pathlib import Path
from mcp.server.fastmcp import FastMCP

# ── Path to the compiled DuckDB file ────────────────────────────────────────
REPO_ROOT = Path(__file__).parent.parent
DB_PATH = REPO_ROOT / "data" / "cx_analytics.duckdb"
SCHEMA = "main_customer_experience"

mcp = FastMCP(
    name="cx-analytics",
    description=(
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


def _safe_query(sql: str) -> list[dict]:
    """Execute a read-only SQL query and return rows as dicts."""
    sql_upper = sql.strip().upper()
    if not sql_upper.startswith("SELECT") and not sql_upper.startswith("WITH"):
        raise ValueError("Only SELECT / WITH queries are allowed.")
    with _connect() as con:
        rel = con.execute(sql)
        cols = [d[0] for d in rel.description]
        return [dict(zip(cols, row)) for row in rel.fetchall()]


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
    return _safe_query(query)


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
        start_month: ISO date string for the start of the range (e.g. '2017-01-01').
        end_month:   ISO date string for the end of the range (e.g. '2017-12-31').

    Returns one row per month within the range, ordered chronologically.
    """
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
        WHERE order_month BETWEEN '{start_month}' AND '{end_month}'
        ORDER BY order_month
    """
    return _safe_query(sql)


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
    filters = ["1=1"]
    if state:
        filters.append(f"state = '{state.upper()}'")
    if order_frequency_segment:
        filters.append(f"order_frequency_segment = '{order_frequency_segment}'")
    if satisfaction_segment:
        filters.append(f"satisfaction_segment = '{satisfaction_segment}'")
    if min_total_spend_brl > 0:
        filters.append(f"total_spend_brl >= {min_total_spend_brl}")

    where = " AND ".join(filters)
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
        WHERE {where}
        ORDER BY total_spend_brl DESC
        LIMIT {limit}
    """
    return _safe_query(sql)


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
    filters = ["1=1"]
    if risk_tier:
        filters.append(f"churn_risk_tier = '{risk_tier}'")
    if state:
        filters.append(f"state = '{state.upper()}'")
    if min_spend_brl > 0:
        filters.append(f"total_spend_brl >= {min_spend_brl}")

    where = " AND ".join(filters)
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
        WHERE {where}
        ORDER BY churn_probability DESC
        LIMIT {limit}
    """
    return _safe_query(sql)


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

    sql = f"""
        SELECT
            {group_by}                                                AS dimension,
            count(*)                                                  AS total_orders,
            round(avg(days_to_deliver), 2)                           AS avg_days_to_deliver,
            round(avg(delivery_delta_days), 2)                       AS avg_delta_vs_estimate,
            round(
                sum(CASE WHEN delivered_on_time THEN 1 ELSE 0 END)::float
                / nullif(count(*), 0) * 100, 1
            )                                                         AS on_time_pct,
            round(avg(review_score), 3)                              AS avg_review_score,
            round(
                sum(CASE WHEN review_score >= 4 THEN 1 ELSE 0 END)::float
                / nullif(count(review_score), 0) * 100, 1
            )                                                         AS csat_pct
        FROM {SCHEMA}.fct_orders
        WHERE order_status = 'delivered'
          AND {group_by} IS NOT NULL
        GROUP BY {group_by}
        HAVING count(*) >= {min_orders}
        ORDER BY avg_days_to_deliver DESC
        LIMIT {limit}
    """
    return _safe_query(sql)


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
    return _safe_query(sql)


# ── Entry point ──────────────────────────────────────────────────────────────

if __name__ == "__main__":
    mcp.run()
