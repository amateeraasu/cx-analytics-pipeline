-- Feature store for churn prediction.
-- Churn = no order placed within 90 days of the customer's last purchase,
-- measured against the latest purchase date in the dataset.
-- Only customers whose first order is >90 days before the reference date are eligible.
with reference as (
    select max(purchased_at) as reference_date
    from {{ ref('fct_orders') }}
),

order_aggs as (
    -- Order-level signals not already aggregated in dim_customers.
    select
        customer_unique_id,
        avg(item_count)                                                       as avg_item_count,
        avg(distinct_products)                                                as avg_distinct_products,
        avg(distinct_sellers)                                                 as avg_distinct_sellers,
        avg(freight_total_brl)                                                as avg_freight_brl,
        {{ bool_or_agg('used_voucher') }}                                     as used_voucher_ever,
        {{ bool_or_agg('used_credit_card') }}                                 as used_credit_card_ever,
        {{ arg_max('primary_payment_type', 'total_payment_value') }}          as primary_payment_type,
        coalesce(min(review_score), 5)                                        as min_review_score,
        cast(count(case when has_review_comment then 1 end) as double)
            / nullif(count(review_score), 0)                                  as pct_reviews_with_comment
    from {{ ref('fct_orders') }}
    where customer_unique_id is not null
    group by customer_unique_id
),

features as (
    select
        -- Identity
        d.customer_unique_id,
        d.customer_sk,

        -- Geography
        d.state,

        -- Recency (RFM: R)
        {{ date_diff_days('d.last_order_at', 'r.reference_date') }}            as days_since_last_order,
        {{ date_diff_days('d.first_order_at', 'r.reference_date') }}          as days_since_first_order,
        d.customer_lifespan_days,
        coalesce(
            cast({{ date_diff_days('d.last_order_at', 'r.reference_date') }} as double)
            / nullif({{ date_diff_days('d.first_order_at', 'r.reference_date') }}, 0),
            1.0
        )                                                                     as recency_to_lifespan_ratio,

        -- Frequency (RFM: F)
        d.total_orders,
        d.delivered_orders,
        d.canceled_orders,
        coalesce(cast(d.canceled_orders as double) / nullif(d.total_orders, 0), 0) as cancel_rate,
        d.order_frequency_segment,

        -- Monetary (RFM: M)
        d.total_spend_brl,
        d.avg_order_value_brl,
        coalesce(o.avg_freight_brl, 0)                                        as avg_freight_brl,
        coalesce(
            o.avg_freight_brl / nullif(d.avg_order_value_brl, 0),
            0
        )                                                                     as freight_to_value_ratio,

        -- Satisfaction signals
        coalesce(d.avg_review_score, 3.0)                                     as avg_review_score,
        d.review_count,
        cast(coalesce(o.min_review_score, 5) <= 2 as int)                    as has_low_review,
        coalesce(o.pct_reviews_with_comment, 0)                               as pct_reviews_with_comment,
        d.satisfaction_segment,

        -- Delivery experience
        coalesce(d.avg_days_to_deliver, 15)                                   as avg_days_to_deliver,
        coalesce(
            cast(d.on_time_deliveries as double) / nullif(d.delivered_orders, 0),
            0
        )                                                                     as on_time_delivery_rate,
        cast(coalesce(d.on_time_deliveries, 0) < coalesce(d.delivered_orders, 1) as int)
                                                                              as had_late_delivery,

        -- Payment behaviour
        cast(coalesce(o.used_voucher_ever, false) as int)                     as used_voucher_ever,
        cast(coalesce(o.used_credit_card_ever, false) as int)                 as used_credit_card_ever,
        coalesce(o.primary_payment_type, 'unknown')                           as primary_payment_type,

        -- Basket composition
        coalesce(o.avg_item_count, 1)                                         as avg_item_count,
        coalesce(o.avg_distinct_products, 1)                                  as avg_distinct_products,
        coalesce(o.avg_distinct_sellers, 1)                                   as avg_distinct_sellers,

        -- Metadata
        r.reference_date,
        (r.reference_date - INTERVAL '90' DAY)                               as churn_cutoff_date,

        -- Target label: 1 = churned, 0 = retained
        case
            when d.last_order_at <= (r.reference_date - INTERVAL '90' DAY) then 1
            else 0
        end                                                                   as is_churned

    from {{ ref('dim_customers') }} d
    cross join reference r
    left join order_aggs o using (customer_unique_id)
    -- Only evaluate customers old enough to have possibly churned
    where d.first_order_at <= (r.reference_date - INTERVAL '90' DAY)
)

select * from features
