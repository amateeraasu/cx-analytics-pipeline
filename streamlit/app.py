"""
CX Analytics Dashboard
======================
Interactive Streamlit dashboard over the CX Analytics DuckDB marts.

Run from the repo root:
    streamlit run streamlit/app.py
"""

import duckdb
import pandas as pd
import plotly.express as px
import streamlit as st
from pathlib import Path

# ── Config ────────────────────────────────────────────────────────────────────

DB_PATH = Path(__file__).parent.parent / "data" / "cx_analytics.duckdb"
SCHEMA = "main_customer_experience"

st.set_page_config(
    page_title="CX Analytics Platform",
    page_icon="📦",
    layout="wide",
)


# ── Connection ────────────────────────────────────────────────────────────────

@st.cache_resource
def get_connection():
    return duckdb.connect(str(DB_PATH), read_only=True)


@st.cache_data(ttl=300)
def query(sql: str) -> pd.DataFrame:
    return get_connection().execute(sql).df()


# ── Header ────────────────────────────────────────────────────────────────────

st.title("📦 CX Analytics Platform")
st.caption(
    "Brazilian e-commerce analytics · 99,441 orders · 2016–2018 · "
    "Built on [Olist](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce) "
    "via dbt + DuckDB"
)

# ── Top-level KPIs ─────────────────────────────────────────────────────────────

kpi_df = query(f"""
    SELECT
        sum(total_orders)                                           AS total_orders,
        round(avg(csat_rate) * 100, 1)                             AS avg_csat_pct,
        round(avg(on_time_rate) * 100, 1)                          AS avg_on_time_pct,
        round(avg(avg_review_score), 2)                            AS avg_review_score,
        round(sum(total_gmv_brl) / 1e6, 2)                        AS total_gmv_m_brl
    FROM {SCHEMA}.cx_satisfaction_summary
""")

c1, c2, c3, c4, c5 = st.columns(5)
c1.metric("Total Orders",        f"{int(kpi_df['total_orders'][0]):,}")
c2.metric("Avg CSAT",            f"{kpi_df['avg_csat_pct'][0]}%")
c3.metric("On-Time Delivery",    f"{kpi_df['avg_on_time_pct'][0]}%")
c4.metric("Avg Review Score",    f"{kpi_df['avg_review_score'][0]} / 5")
c5.metric("Total GMV",           f"R$ {kpi_df['total_gmv_m_brl'][0]}M")

st.divider()

# ── Tabs ──────────────────────────────────────────────────────────────────────

tab1, tab2, tab3, tab4 = st.tabs(["📈 Monthly KPIs", "🚚 Delivery by State", "⚠️ Churn Risk", "🔍 Audit Log"])


# ──────────────────────────────────────────────────────────────────────────────
# TAB 1 · Monthly KPIs
# ──────────────────────────────────────────────────────────────────────────────

with tab1:
    monthly = query(f"""
        SELECT
            order_month::DATE                          AS month,
            total_orders,
            round(csat_rate * 100, 2)                  AS csat_pct,
            round(on_time_rate * 100, 2)               AS on_time_pct,
            avg_review_score,
            round(total_gmv_brl / 1000, 1)             AS gmv_k_brl,
            avg_days_to_deliver,
            low_score_orders
        FROM {SCHEMA}.cx_satisfaction_summary
        ORDER BY month
    """)

    col_l, col_r = st.columns(2)

    with col_l:
        st.subheader("CSAT & On-Time Rate")
        fig = px.line(
            monthly, x="month",
            y=["csat_pct", "on_time_pct"],
            labels={"value": "%", "variable": "Metric", "month": "Month"},
            color_discrete_map={"csat_pct": "#2ecc71", "on_time_pct": "#3498db"},
        )
        fig.for_each_trace(lambda t: t.update(
            name={"csat_pct": "CSAT Rate", "on_time_pct": "On-Time Rate"}[t.name]
        ))
        fig.update_layout(legend_title="", height=340)
        st.plotly_chart(fig, use_container_width=True)

    with col_r:
        st.subheader("Monthly GMV (R$ thousands)")
        fig2 = px.bar(
            monthly, x="month", y="gmv_k_brl",
            labels={"gmv_k_brl": "GMV (R$ k)", "month": "Month"},
            color_discrete_sequence=["#9b59b6"],
        )
        fig2.update_layout(height=340)
        st.plotly_chart(fig2, use_container_width=True)

    col_l2, col_r2 = st.columns(2)

    with col_l2:
        st.subheader("Avg Review Score")
        fig3 = px.line(
            monthly, x="month", y="avg_review_score",
            labels={"avg_review_score": "Score (1–5)", "month": "Month"},
            color_discrete_sequence=["#e67e22"],
        )
        fig3.update_layout(yaxis_range=[1, 5], height=300)
        st.plotly_chart(fig3, use_container_width=True)

    with col_r2:
        st.subheader("Avg Days to Deliver")
        fig4 = px.line(
            monthly, x="month", y="avg_days_to_deliver",
            labels={"avg_days_to_deliver": "Days", "month": "Month"},
            color_discrete_sequence=["#e74c3c"],
        )
        fig4.update_layout(height=300)
        st.plotly_chart(fig4, use_container_width=True)


# ──────────────────────────────────────────────────────────────────────────────
# TAB 2 · Delivery by State
# ──────────────────────────────────────────────────────────────────────────────

with tab2:
    states = query(f"""
        SELECT
            c.state,
            count(*)                                                        AS orders,
            round(avg(f.days_to_deliver), 1)                               AS avg_days,
            round(avg(f.delivery_delta_days), 1)                           AS avg_delta,
            round(
                sum(CASE WHEN f.delivered_on_time THEN 1 ELSE 0 END)::float
                / nullif(count(*), 0) * 100, 1
            )                                                               AS on_time_pct,
            round(avg(f.review_score), 2)                                  AS avg_review,
            round(
                sum(CASE WHEN f.review_score >= 4 THEN 1 ELSE 0 END)::float
                / nullif(count(f.review_score), 0) * 100, 1
            )                                                               AS csat_pct
        FROM {SCHEMA}.fct_orders f
        JOIN {SCHEMA}.dim_customers c USING (customer_sk)
        WHERE f.order_status = 'delivered' AND c.state IS NOT NULL
        GROUP BY c.state
        HAVING count(*) >= 100
        ORDER BY avg_days DESC
    """)

    col_l, col_r = st.columns([2, 1])

    with col_l:
        sort_by = st.selectbox(
            "Sort by",
            ["avg_days", "on_time_pct", "avg_review", "csat_pct", "orders"],
            format_func=lambda x: {
                "avg_days": "Avg Days to Deliver",
                "on_time_pct": "On-Time %",
                "avg_review": "Avg Review Score",
                "csat_pct": "CSAT %",
                "orders": "Order Volume",
            }[x],
        )
        ascending = sort_by in ["avg_days"]
        sorted_df = states.sort_values(sort_by, ascending=ascending)

        fig5 = px.bar(
            sorted_df, x="state", y=sort_by,
            color=sort_by,
            color_continuous_scale="RdYlGn" if sort_by != "avg_days" else "RdYlGn_r",
            labels={"state": "State", sort_by: sort_by.replace("_", " ").title()},
            height=420,
        )
        fig5.update_layout(coloraxis_showscale=False)
        st.plotly_chart(fig5, use_container_width=True)

    with col_r:
        st.subheader("State Summary")
        display = states[["state", "orders", "avg_days", "on_time_pct", "avg_review"]].copy()
        display.columns = ["State", "Orders", "Avg Days", "On-Time %", "Review"]
        st.dataframe(
            display.style.background_gradient(
                subset=["Avg Days"], cmap="RdYlGn_r"
            ).background_gradient(
                subset=["On-Time %"], cmap="RdYlGn"
            ),
            use_container_width=True,
            height=420,
            hide_index=True,
        )

    st.subheader("Delivery vs. Satisfaction")
    fig6 = px.scatter(
        states, x="avg_days", y="avg_review",
        size="orders", color="on_time_pct",
        text="state",
        labels={
            "avg_days": "Avg Days to Deliver",
            "avg_review": "Avg Review Score",
            "on_time_pct": "On-Time %",
            "orders": "Order Volume",
        },
        color_continuous_scale="RdYlGn",
        height=450,
    )
    fig6.update_traces(textposition="top center", textfont_size=10)
    fig6.update_layout(coloraxis_colorbar_title="On-Time %")
    st.plotly_chart(fig6, use_container_width=True)
    st.caption("Bubble size = order volume. The negative correlation between delivery time and review score is visible across all states.")


# ──────────────────────────────────────────────────────────────────────────────
# TAB 3 · Churn Risk
# ──────────────────────────────────────────────────────────────────────────────

with tab3:
    churn_path = Path(__file__).parent.parent / "data" / "churn_predictions.csv"

    if not churn_path.exists():
        st.info(
            "Churn predictions not yet generated. "
            "Run `notebooks/churn_prediction.ipynb` first, then refresh.",
            icon="ℹ️",
        )
    else:
        churn = query(f"""
            SELECT
                customer_unique_id,
                round(churn_probability, 3)     AS churn_prob,
                churn_risk_tier,
                state,
                order_frequency_segment         AS freq_segment,
                satisfaction_segment            AS sat_segment,
                total_orders,
                round(total_spend_brl, 2)       AS spend_brl,
                round(avg_review_score, 2)      AS avg_review,
                last_order_at::DATE             AS last_order
            FROM {SCHEMA}.mart_churn_predictions
            ORDER BY churn_prob DESC
        """)

        col_l, col_r = st.columns(2)

        with col_l:
            tier_counts = churn["churn_risk_tier"].value_counts().reset_index()
            tier_counts.columns = ["Risk Tier", "Customers"]
            tier_order = ["critical", "high", "medium", "low"]
            tier_counts["Risk Tier"] = pd.Categorical(
                tier_counts["Risk Tier"], categories=tier_order, ordered=True
            )
            tier_counts = tier_counts.sort_values("Risk Tier")

            fig7 = px.bar(
                tier_counts, x="Risk Tier", y="Customers",
                color="Risk Tier",
                color_discrete_map={
                    "critical": "#e74c3c",
                    "high": "#e67e22",
                    "medium": "#f1c40f",
                    "low": "#2ecc71",
                },
                title="Customers by Churn Risk Tier",
                height=350,
            )
            fig7.update_layout(showlegend=False)
            st.plotly_chart(fig7, use_container_width=True)

        with col_r:
            fig8 = px.histogram(
                churn, x="churn_prob", nbins=40,
                color_discrete_sequence=["#3498db"],
                title="Churn Probability Distribution",
                labels={"churn_prob": "Churn Probability"},
                height=350,
            )
            st.plotly_chart(fig8, use_container_width=True)

        st.subheader("High-Value Customers at Risk")
        st.caption("Customers with spend > R$200 and churn probability > 0.5 — prioritise these for retention outreach.")

        at_risk = churn[
            (churn["spend_brl"] > 200) & (churn["churn_prob"] > 0.5)
        ].head(50)

        st.dataframe(
            at_risk[["customer_unique_id", "churn_risk_tier", "churn_prob",
                      "spend_brl", "total_orders", "avg_review", "state", "last_order"]]
            .rename(columns={
                "customer_unique_id": "Customer ID",
                "churn_risk_tier": "Risk Tier",
                "churn_prob": "Churn Prob",
                "spend_brl": "Spend (BRL)",
                "total_orders": "Orders",
                "avg_review": "Avg Review",
                "state": "State",
                "last_order": "Last Order",
            })
            .style.background_gradient(subset=["Churn Prob"], cmap="Reds"),
            use_container_width=True,
            hide_index=True,
        )

        col1, col2 = st.columns(2)
        with col1:
            fig9 = px.box(
                churn, x="freq_segment", y="churn_prob",
                color="freq_segment",
                title="Churn Probability by Purchase Frequency",
                labels={"freq_segment": "Segment", "churn_prob": "Churn Probability"},
                category_orders={"freq_segment": ["one_time", "repeat", "loyal"]},
                height=350,
            )
            fig9.update_layout(showlegend=False)
            st.plotly_chart(fig9, use_container_width=True)

        with col2:
            fig10 = px.box(
                churn, x="sat_segment", y="churn_prob",
                color="sat_segment",
                title="Churn Probability by Satisfaction Segment",
                labels={"sat_segment": "Segment", "churn_prob": "Churn Probability"},
                category_orders={"sat_segment": ["satisfied", "neutral", "dissatisfied"]},
                color_discrete_map={
                    "satisfied": "#2ecc71",
                    "neutral": "#f1c40f",
                    "dissatisfied": "#e74c3c",
                },
                height=350,
            )
            fig9.update_layout(showlegend=False)
            st.plotly_chart(fig10, use_container_width=True)


# ──────────────────────────────────────────────────────────────────────────────
# TAB 4 · Audit Log
# ──────────────────────────────────────────────────────────────────────────────

LOG_PATH = Path(__file__).parent.parent / "logs" / "mcp_audit.log"

with tab4:
    st.subheader("MCP Server Audit Log")
    st.caption("Every query executed by the MCP server — tool name, parameters, row count, duration, status.")

    if not LOG_PATH.exists():
        st.info("No audit log found. Run the MCP server and make some queries first.", icon="ℹ️")
    else:
        auto_refresh = st.toggle("Auto-refresh every 5 seconds", value=False)
        if auto_refresh:
            st.write("🟢 Live — refreshing every 5s")

        # Read JSON Lines log — INFO level only (skip DEBUG)
        rows = []
        with open(LOG_PATH) as f:
            for line in f:
                try:
                    import json
                    entry = json.loads(line)
                    if entry.get("level") == "INFO" and entry.get("event") == "data_access":
                        rows.append({
                            "timestamp":   entry.get("timestamp", "")[:19].replace("T", " "),
                            "session":     entry.get("session", ""),
                            "function":    entry.get("function", ""),
                            "row_count":   entry.get("row_count", 0),
                            "duration_ms": entry.get("duration_ms", 0),
                            "status":      entry.get("status", ""),
                            "params":      str(entry.get("params", "")),
                        })
                except Exception:
                    continue

        if not rows:
            st.info("Log file exists but has no data access entries yet.")
        else:
            log_df = pd.DataFrame(rows)
            log_df["timestamp"] = pd.to_datetime(log_df["timestamp"])
            log_df = log_df.sort_values("timestamp", ascending=False)

            # ── Summary KPIs ──────────────────────────────────────────────────
            k1, k2, k3, k4 = st.columns(4)
            k1.metric("Total Queries",    len(log_df))
            k2.metric("Avg Duration",     f"{log_df['duration_ms'].mean():.1f} ms")
            k3.metric("Errors",           int((log_df["status"] == "error").sum()))
            k4.metric("Unique Sessions",  log_df["session"].nunique())

            st.divider()

            # ── Charts ────────────────────────────────────────────────────────
            col_l, col_r = st.columns(2)

            with col_l:
                calls_by_fn = log_df["function"].value_counts().reset_index()
                calls_by_fn.columns = ["function", "calls"]
                fig_a = px.bar(
                    calls_by_fn, x="function", y="calls",
                    title="Calls per Tool",
                    color="calls",
                    color_continuous_scale="Blues",
                    height=320,
                )
                fig_a.update_layout(coloraxis_showscale=False, showlegend=False)
                st.plotly_chart(fig_a, use_container_width=True)

            with col_r:
                avg_dur = log_df.groupby("function")["duration_ms"].mean().reset_index()
                avg_dur.columns = ["function", "avg_ms"]
                avg_dur = avg_dur.sort_values("avg_ms", ascending=False)
                fig_b = px.bar(
                    avg_dur, x="function", y="avg_ms",
                    title="Avg Duration per Tool (ms)",
                    color="avg_ms",
                    color_continuous_scale="Oranges",
                    height=320,
                )
                fig_b.update_layout(coloraxis_showscale=False)
                st.plotly_chart(fig_b, use_container_width=True)

            # ── Filters ───────────────────────────────────────────────────────
            st.subheader("Query History")
            col_f1, col_f2 = st.columns(2)
            with col_f1:
                fn_filter = st.multiselect(
                    "Filter by tool",
                    options=sorted(log_df["function"].unique()),
                    default=[],
                )
            with col_f2:
                status_filter = st.multiselect(
                    "Filter by status",
                    options=["success", "error"],
                    default=[],
                )

            filtered = log_df.copy()
            if fn_filter:
                filtered = filtered[filtered["function"].isin(fn_filter)]
            if status_filter:
                filtered = filtered[filtered["status"].isin(status_filter)]

            # ── Table ─────────────────────────────────────────────────────────
            st.dataframe(
                filtered[["timestamp", "function", "row_count", "duration_ms", "status", "session", "params"]]
                .rename(columns={
                    "timestamp":   "Timestamp",
                    "function":    "Tool",
                    "row_count":   "Rows",
                    "duration_ms": "Duration (ms)",
                    "status":      "Status",
                    "session":     "Session",
                    "params":      "Params",
                })
                .style.map(
                    lambda v: "color: #e74c3c" if v == "error" else "",
                    subset=["Status"]
                ),
                use_container_width=True,
                hide_index=True,
                height=400,
            )

        if auto_refresh:
            import time
            time.sleep(5)
            st.rerun()
