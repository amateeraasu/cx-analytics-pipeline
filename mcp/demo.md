# MCP Demo: Querying CX Analytics with Claude

Once the server is running and connected to Claude Desktop, you can ask questions
in plain English. Claude translates them into tool calls against the DuckDB marts.

---

## Setup

### 1. Install dependencies

```bash
cd mcp
pip install -r requirements.txt
```

### 2. Build the dbt pipeline first (if you haven't already)

```bash
# From repo root
dbt run --profiles-dir .
dbt test --profiles-dir .
```

### 3. Add to Claude Desktop

Edit `~/Library/Application Support/Claude/claude_desktop_config.json`:

```json
{
  "mcpServers": {
    "cx-analytics": {
      "command": "python",
      "args": ["/absolute/path/to/cx-analytics-pipeline/mcp/server.py"]
    }
  }
}
```

Restart Claude Desktop. You should see `cx-analytics` in the tools panel.

---

## Example Questions

### Monthly KPIs

> "What was the CSAT rate trend across 2017? Did it improve over the year?"

→ Claude calls `get_monthly_kpis(start_month="2017-01-01", end_month="2017-12-31")`

---

### Delivery performance

> "Which Brazilian states have the worst on-time delivery rates? Show me the bottom 10."

→ Claude calls `get_delivery_performance(group_by="state", limit=10)` and sorts by on_time_pct ascending

---

### Customer segments

> "Show me dissatisfied customers in São Paulo who've spent more than 500 BRL."

→ Claude calls `get_customer_segments(state="SP", satisfaction_segment="dissatisfied", min_total_spend_brl=500)`

---

### Churn risk

> "Which high-risk churn customers have spent the most? I want to prioritise outreach."

→ Claude calls `get_churn_risk(risk_tier="high", limit=20)`

---

### Custom SQL

> "How does average review score vary by day of week? Are weekend orders rated differently?"

→ Claude calls `run_sql(...)` with a GROUP BY query on `order_day_of_week`

---

### Schema exploration

> "What columns are available in the fct_orders table?"

→ Claude calls `list_tables()` and filters to fct_orders

---

## Available Tools

| Tool | What it does |
|---|---|
| `run_sql` | Execute any read-only SQL against DuckDB |
| `get_monthly_kpis` | Monthly CSAT, on-time rate, GMV for a date range |
| `get_customer_segments` | Filter dim_customers by state, segment, spend |
| `get_churn_risk` | Churn predictions filtered by risk tier, state, spend |
| `get_delivery_performance` | Delivery + satisfaction grouped by any dimension |
| `list_tables` | Show all tables and columns (schema explorer) |

All tools are **read-only** — no writes to the database are possible.
