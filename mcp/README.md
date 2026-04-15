# CX Analytics MCP Server

Connects Claude to the CX Analytics DuckDB database via the [Model Context Protocol](https://modelcontextprotocol.io/). Once configured, you can query 100K+ orders, customer segments, churn predictions, and delivery metrics using plain English — no SQL required.

## What you can ask

```
"What was the CSAT trend across 2017?"
"Which states have the worst on-time delivery rates?"
"Show me dissatisfied customers in SP who've spent more than R$500."
"Who are the top 20 high-risk churn customers by lifetime spend?"
"Run a cohort analysis — how does repeat purchase rate vary by acquisition month?"
```

Claude translates natural language into parameterised DuckDB queries, returns structured results, and explains what it found.

---

## Available tools

| Tool | What it does |
|---|---|
| `run_sql` | Execute any read-only SQL against the DuckDB marts |
| `get_monthly_kpis` | CSAT rate, on-time rate, GMV, avg review score for a date range |
| `get_customer_segments` | Filter `dim_customers` by state, segment, and minimum spend |
| `get_churn_risk` | Churn predictions filtered by risk tier, state, and spend |
| `get_delivery_performance` | Delivery metrics grouped by state, month, day-of-week, or payment type |
| `list_tables` | Show all tables and their columns (schema explorer) |

All tools are **read-only** — no writes to the database are ever possible.

---

## Setup

### Prerequisites

- Python 3.10–3.12
- The dbt pipeline run at least once (`dbt run --profiles-dir .`) so `data/cx_analytics.duckdb` exists
- [Claude Desktop](https://claude.ai/download) installed

### 1. Install dependencies

```bash
# From the repo root
pip install -r mcp/requirements.txt
```

### 2. Edit `mcp/config.json`

Replace the placeholder path with the absolute path to `server.py` on your machine:

```json
{
  "mcpServers": {
    "cx-analytics": {
      "command": "python",
      "args": ["/Users/yourname/cx-analytics-pipeline/mcp/server.py"]
    }
  }
}
```

### 3. Add to Claude Desktop

Copy the content of `mcp/config.json` into Claude Desktop's config file:

**macOS:** `~/Library/Application Support/Claude/claude_desktop_config.json`
**Windows:** `%APPDATA%\Claude\claude_desktop_config.json`

If the config file already has other MCP servers, add the `cx-analytics` key inside the existing `mcpServers` object.

### 4. Restart Claude Desktop

After restarting, you should see `cx-analytics` in Claude Desktop's tools panel (the hammer icon). The server is ready when Claude can list available tools.

### 5. Test the connection

Ask Claude: _"List the available tables in the CX Analytics database."_

Claude should call `list_tables()` and return the 4 mart tables with their columns.

---

## Example prompts

**Monthly KPI trend**
> "What was the CSAT rate trend across 2017? Did it improve month-over-month?"

→ `get_monthly_kpis(start_month="2017-01-01", end_month="2017-12-31")`

**Delivery performance**
> "Which Brazilian states have the worst on-time delivery rates? Show me the bottom 10."

→ `get_delivery_performance(group_by="state", limit=10)` sorted ascending by on_time_pct

**Customer segmentation**
> "How many customers are in each frequency segment (one-time, repeat, loyal)? What's the average spend per segment?"

→ `run_sql(...)` with a GROUP BY on `order_frequency_segment`

**Churn targeting**
> "Which critical-risk churn customers in São Paulo have spent the most? I want to prioritise outreach."

→ `get_churn_risk(risk_tier="critical", state="SP", limit=20)`

**Cohort analysis**
> "Run a cohort analysis — group customers by their first purchase month and show average lifetime spend for each cohort."

→ `run_sql(...)` with a cohort query using `first_order_at` from `dim_customers`

---

## Security

The server applies several defences against SQL injection and data exposure:

- **Parameterised queries** — all user-supplied filter values are bound as `?` parameters, never interpolated into SQL strings
- **`group_by` allowlist** — `get_delivery_performance` validates the dimension column against an explicit set; no arbitrary column names accepted
- **Date range validation** — date inputs are parsed with `datetime.date.fromisoformat()` (rejects impossible dates) and bounded to the dataset window (2016–2018)
- **Read-only connection** — `duckdb.connect(..., read_only=True)` — writes are structurally impossible
- **PII masking** — `customer_unique_id` values in `get_customer_segments` and `get_churn_risk` are partially masked in the response
- **Audit logging** — every tool call is timestamped and logged with query preview, parameter values, and row count

---

## Troubleshooting

**"DuckDB file not found"**
Run `dbt run --profiles-dir .` from the repo root first. The pipeline writes to `data/cx_analytics.duckdb`.

**Server not appearing in Claude Desktop**
Check the path in your config — it must be an absolute path. Verify `python mcp/server.py` runs without error from your terminal first.

**`mart_churn_predictions` not available**
Run `notebooks/churn_prediction.ipynb` to generate `data/churn_predictions.csv`, then re-run `dbt run --profiles-dir .`.
