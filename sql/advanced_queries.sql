-- =============================================================================
-- CX Analytics — Advanced SQL Showcase
-- =============================================================================
-- These queries run against the DuckDB mart tables produced by the dbt pipeline.
-- Run them via:  duckdb data/cx_analytics.duckdb < sql/advanced_queries.sql
-- Or paste into any DuckDB-compatible SQL client.
--
-- Schema: main_customer_experience
-- Tables: fct_orders, dim_customers, cx_satisfaction_summary, mart_churn_predictions
-- =============================================================================


-- =============================================================================
-- QUERY 1: Month-over-Month CSAT trend with LAG and period-over-period delta
-- =============================================================================
-- Business question: Is customer satisfaction improving or declining?
-- Technique: LAG window function for period-over-period comparison

WITH monthly_kpis AS (
    SELECT
        order_month::DATE                                   AS month,
        total_orders,
        ROUND(csat_rate * 100, 1)                           AS csat_pct,
        ROUND(on_time_rate * 100, 1)                        AS on_time_pct,
        ROUND(avg_review_score, 2)                          AS avg_review,
        ROUND(total_gmv_brl / 1000, 1)                      AS gmv_k_brl
    FROM main_customer_experience.cx_satisfaction_summary
),

with_deltas AS (
    SELECT
        month,
        total_orders,
        csat_pct,
        on_time_pct,
        avg_review,
        gmv_k_brl,
        -- LAG: compare each month to the previous month
        LAG(csat_pct)    OVER (ORDER BY month) AS prev_csat_pct,
        LAG(on_time_pct) OVER (ORDER BY month) AS prev_on_time_pct,
        LAG(gmv_k_brl)   OVER (ORDER BY month) AS prev_gmv_k_brl
    FROM monthly_kpis
)

SELECT
    month,
    total_orders,
    csat_pct,
    ROUND(csat_pct - prev_csat_pct, 1)       AS csat_delta_pp,     -- percentage points
    on_time_pct,
    ROUND(on_time_pct - prev_on_time_pct, 1) AS on_time_delta_pp,
    gmv_k_brl,
    ROUND(
        (gmv_k_brl - prev_gmv_k_brl) / NULLIF(prev_gmv_k_brl, 0) * 100, 1
    )                                         AS gmv_growth_pct
FROM with_deltas
ORDER BY month;


-- =============================================================================
-- QUERY 2: Running GMV total and cumulative order count
-- =============================================================================
-- Business question: How does cumulative revenue build over time?
-- Technique: SUM OVER with ROWS BETWEEN (running window)

SELECT
    order_month::DATE                               AS month,
    total_orders,
    ROUND(total_gmv_brl / 1000, 1)                 AS gmv_k_brl,

    -- Cumulative totals from the beginning of the dataset
    SUM(total_orders)  OVER (ORDER BY order_month
                             ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
                        )                           AS cumulative_orders,

    ROUND(
        SUM(total_gmv_brl) OVER (ORDER BY order_month
                                 ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
                            ) / 1000, 1
    )                                               AS cumulative_gmv_k_brl,

    -- Running average review score (weighted by order volume)
    ROUND(
        SUM(avg_review_score * total_orders) OVER (ORDER BY order_month
                                                   ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
        / NULLIF(SUM(total_orders) OVER (ORDER BY order_month
                                         ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW), 0),
        3
    )                                               AS running_avg_review

FROM main_customer_experience.cx_satisfaction_summary
ORDER BY order_month;


-- =============================================================================
-- QUERY 3: Customer spending percentiles and segment ranking
-- =============================================================================
-- Business question: How does spend distribute across the customer base?
--                   Who are the top-decile customers?
-- Technique: NTILE + PERCENT_RANK window functions, CASE WHEN segmentation

WITH customer_spend AS (
    SELECT
        customer_unique_id,
        state,
        total_orders,
        ROUND(total_spend_brl, 2)           AS total_spend_brl,
        avg_review_score,
        order_frequency_segment,
        satisfaction_segment,

        -- Spend decile (1 = bottom 10%, 10 = top 10%)
        NTILE(10) OVER (ORDER BY total_spend_brl)       AS spend_decile,

        -- Percentile rank (0.0 to 1.0)
        ROUND(PERCENT_RANK() OVER (ORDER BY total_spend_brl), 4)
                                                         AS spend_percentile,

        -- Rank within their state
        RANK() OVER (
            PARTITION BY state
            ORDER BY total_spend_brl DESC
        )                                               AS rank_in_state

    FROM main_customer_experience.dim_customers
    WHERE total_spend_brl > 0
)

SELECT
    customer_unique_id,
    state,
    total_orders,
    total_spend_brl,
    avg_review_score,
    order_frequency_segment,
    satisfaction_segment,
    spend_decile,
    spend_percentile,
    rank_in_state,

    -- Business label based on decile
    CASE
        WHEN spend_decile = 10 THEN 'Top 10% spender'
        WHEN spend_decile >= 8 THEN 'High spender'
        WHEN spend_decile >= 5 THEN 'Mid spender'
        ELSE                        'Low spender'
    END                             AS spend_tier

FROM customer_spend
WHERE spend_decile = 10   -- Focus on top-decile customers
ORDER BY total_spend_brl DESC
LIMIT 100;


-- =============================================================================
-- QUERY 4: Delivery speed vs. satisfaction — cohort analysis by delivery bucket
-- =============================================================================
-- Business question: At what delivery time does satisfaction drop off a cliff?
-- Technique: CASE WHEN bucketing, aggregation over buckets, correlated ordering

SELECT
    -- Bucket orders by days to deliver
    CASE
        WHEN days_to_deliver <= 3  THEN '1. ≤ 3 days (fast)'
        WHEN days_to_deliver <= 7  THEN '2. 4–7 days (standard)'
        WHEN days_to_deliver <= 14 THEN '3. 8–14 days (slow)'
        WHEN days_to_deliver <= 21 THEN '4. 15–21 days (very slow)'
        ELSE                            '5. 22+ days (extreme)'
    END                                             AS delivery_bucket,

    COUNT(*)                                        AS total_orders,
    ROUND(AVG(review_score), 3)                     AS avg_review_score,
    ROUND(AVG(days_to_deliver), 1)                  AS avg_days_actual,

    -- CSAT = share of reviews scoring 4 or 5
    ROUND(
        COUNT(CASE WHEN review_score >= 4 THEN 1 END)::FLOAT
        / NULLIF(COUNT(review_score), 0) * 100, 1
    )                                               AS csat_pct,

    -- On-time rate within bucket
    ROUND(
        COUNT(CASE WHEN delivered_on_time THEN 1 END)::FLOAT
        / NULLIF(COUNT(*), 0) * 100, 1
    )                                               AS on_time_pct,

    -- Share of orders in each bucket
    ROUND(COUNT(*)::FLOAT / SUM(COUNT(*)) OVER () * 100, 1) AS pct_of_orders

FROM main_customer_experience.fct_orders
WHERE order_status = 'delivered'
  AND days_to_deliver IS NOT NULL
  AND review_score IS NOT NULL
GROUP BY delivery_bucket
ORDER BY delivery_bucket;


-- =============================================================================
-- QUERY 5: First-purchase cohort retention — who came back?
-- =============================================================================
-- Business question: Of customers who first ordered in a given month,
--                   how many eventually placed a second order?
-- Technique: Multi-step CTE, self-join for cohort analysis, ROW_NUMBER

WITH customer_orders_ranked AS (
    -- Rank every order per customer chronologically
    SELECT
        customer_unique_id,
        order_id,
        purchased_at,
        DATE_TRUNC('month', purchased_at)::DATE AS order_month,
        ROW_NUMBER() OVER (
            PARTITION BY customer_unique_id
            ORDER BY purchased_at
        )                                        AS order_rank
    FROM main_customer_experience.fct_orders
    WHERE order_status IN ('delivered', 'shipped', 'processing', 'approved')
),

first_orders AS (
    SELECT
        customer_unique_id,
        order_month AS cohort_month
    FROM customer_orders_ranked
    WHERE order_rank = 1
),

second_orders AS (
    SELECT DISTINCT customer_unique_id
    FROM customer_orders_ranked
    WHERE order_rank = 2
),

cohort_sizes AS (
    SELECT
        cohort_month,
        COUNT(*)                                         AS cohort_size,
        COUNT(s.customer_unique_id)                      AS returned_customers,
        ROUND(
            COUNT(s.customer_unique_id)::FLOAT
            / NULLIF(COUNT(*), 0) * 100, 2
        )                                               AS retention_pct
    FROM first_orders f
    LEFT JOIN second_orders s USING (customer_unique_id)
    GROUP BY cohort_month
    HAVING COUNT(*) >= 50   -- Only cohorts with enough data to be meaningful
)

SELECT
    cohort_month,
    cohort_size,
    returned_customers,
    retention_pct,
    -- Context: Olist is a single-purchase marketplace, so even 1–2% retention is notable
    CASE
        WHEN retention_pct >= 2.0 THEN 'Above average retention'
        WHEN retention_pct >= 1.0 THEN 'Average retention'
        ELSE                           'Below average retention'
    END                                AS retention_label
FROM cohort_sizes
ORDER BY cohort_month;


-- =============================================================================
-- QUERY 6: High-value customers at churn risk — retention targeting list
-- =============================================================================
-- Business question: Which customers should the retention team contact first?
--                   (High spend + high churn probability)
-- Technique: Multi-join CTE, business scoring formula, subquery filtering

WITH churn_with_context AS (
    SELECT
        cp.customer_unique_id,
        ROUND(cp.churn_probability, 3)      AS churn_prob,
        cp.churn_risk_tier,
        dc.state,
        dc.city,
        dc.total_orders,
        ROUND(dc.total_spend_brl, 2)        AS total_spend_brl,
        ROUND(dc.avg_order_value_brl, 2)    AS avg_order_value_brl,
        ROUND(dc.avg_review_score, 2)       AS avg_review_score,
        dc.order_frequency_segment,
        dc.satisfaction_segment,
        dc.last_order_at::DATE              AS last_order_date,
        -- Days since last order (using dataset reference date 2018-10-17)
        DATE_DIFF('day', dc.last_order_at, DATE '2018-10-17') AS days_inactive
    FROM main_customer_experience.mart_churn_predictions cp
    JOIN main_customer_experience.dim_customers dc USING (customer_unique_id)
),

-- Score each customer: weight high spend + high churn probability
scored AS (
    SELECT
        *,
        -- Composite retention priority score (higher = more urgent to contact)
        ROUND(
            (churn_prob * 0.5)                                          -- 50% weight: churn risk
            + (total_spend_brl / NULLIF(
                  MAX(total_spend_brl) OVER (), 0) * 0.35)              -- 35% weight: spend value
            + (CASE order_frequency_segment
                   WHEN 'loyal'   THEN 0.15
                   WHEN 'repeat'  THEN 0.10
                   ELSE           0.0
               END),                                                    -- 15% weight: loyalty tier
            4
        )                                                               AS retention_priority_score
    FROM churn_with_context
    WHERE total_spend_brl >= 100   -- Only customers worth retaining spend
)

SELECT
    customer_unique_id,
    churn_prob,
    churn_risk_tier,
    state,
    total_orders,
    total_spend_brl,
    avg_review_score,
    order_frequency_segment,
    satisfaction_segment,
    last_order_date,
    days_inactive,
    ROUND(retention_priority_score, 3) AS priority_score,
    -- Recommended action based on tier + satisfaction
    CASE
        WHEN churn_risk_tier IN ('critical', 'high') AND satisfaction_segment = 'satisfied'
            THEN 'Win-back: satisfied but inactive — offer loyalty discount'
        WHEN churn_risk_tier IN ('critical', 'high') AND satisfaction_segment = 'dissatisfied'
            THEN 'Service recovery: address complaint before re-engagement'
        WHEN churn_risk_tier = 'medium'
            THEN 'Nurture: send re-engagement email with personalised offer'
        ELSE
            'Monitor: include in loyalty program communications'
    END                                AS recommended_action
FROM scored
ORDER BY retention_priority_score DESC
LIMIT 50;


-- =============================================================================
-- QUERY 7: Seller delivery performance — window ranking within categories
-- =============================================================================
-- Business question: Which sellers have the worst delivery times relative to
--                   others selling in the same product category?
-- Technique: Window function with PARTITION BY for within-group ranking,
--            HAVING to filter noise, nested CTEs

WITH seller_category_perf AS (
    -- Join orders + items + category translation to get seller × category grain
    SELECT
        i.seller_id,
        COALESCE(cat.product_category_name_english, p.product_category_name, 'unknown')
                                                    AS category_en,
        COUNT(DISTINCT f.order_id)                  AS orders,
        ROUND(AVG(f.days_to_deliver), 1)            AS avg_delivery_days,
        ROUND(AVG(f.review_score), 2)               AS avg_review,
        ROUND(
            COUNT(CASE WHEN f.delivered_on_time THEN 1 END)::FLOAT
            / NULLIF(COUNT(f.order_id), 0) * 100, 1
        )                                           AS on_time_pct
    FROM main_customer_experience.fct_orders f
    JOIN main_olist.stg_order_items i  USING (order_id)
    JOIN main_olist.stg_products p     USING (product_id)
    LEFT JOIN main_olist.stg_geolocation cat
        ON p.product_category_name = cat.product_category_name   -- reuse for translation
    WHERE f.order_status = 'delivered'
      AND f.days_to_deliver IS NOT NULL
    GROUP BY i.seller_id, category_en
    HAVING COUNT(DISTINCT f.order_id) >= 20   -- minimum volume for reliable stats
),

ranked AS (
    SELECT
        *,
        -- Rank sellers by avg delivery days WITHIN each category
        RANK() OVER (
            PARTITION BY category_en
            ORDER BY avg_delivery_days DESC   -- worst = rank 1
        )                                    AS delivery_rank_in_category,

        -- How far is this seller from the category median?
        ROUND(
            avg_delivery_days
            - PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY avg_delivery_days)
              OVER (PARTITION BY category_en),
            1
        )                                    AS days_above_category_median,

        COUNT(*) OVER (PARTITION BY category_en) AS sellers_in_category
    FROM seller_category_perf
)

SELECT
    seller_id,
    category_en,
    orders,
    avg_delivery_days,
    on_time_pct,
    avg_review,
    delivery_rank_in_category,
    days_above_category_median,
    sellers_in_category,
    -- Flag the worst performers
    CASE
        WHEN delivery_rank_in_category = 1
         AND days_above_category_median >= 3 THEN 'Underperformer: action needed'
        WHEN days_above_category_median >= 1.5 THEN 'Watch: above category average'
        ELSE                                       'On track'
    END                                            AS performance_flag
FROM ranked
WHERE delivery_rank_in_category <= 5       -- Top 5 worst sellers per category
ORDER BY category_en, delivery_rank_in_category;
