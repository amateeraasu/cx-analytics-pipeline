with orders as (
    select * from {{ ref('int_orders_enriched') }}
),

customers as (
    select customer_id, customer_unique_id
    from {{ ref('stg_customers') }}
),

items as (
    select
        order_id,
        count(*)                    as item_count,
        count(distinct product_id)  as distinct_products,
        count(distinct seller_id)   as distinct_sellers,
        sum(price)                  as items_subtotal_brl,
        sum(freight_value)          as freight_total_brl
    from {{ ref('stg_order_items') }}
    group by order_id
),

dim as (
    select customer_unique_id, customer_sk
    from {{ ref('dim_customers') }}
),

fct as (
    select
        o.order_id,
        c.customer_unique_id,
        d.customer_sk,

        o.order_status,
        o.purchased_at,
        o.approved_at,
        o.customer_delivered_at,
        o.estimated_delivery_at,

        -- Items
        i.item_count,
        i.distinct_products,
        i.distinct_sellers,
        i.items_subtotal_brl,
        i.freight_total_brl,

        -- Payment
        o.total_payment_value,
        o.primary_payment_type,
        o.used_voucher,
        o.used_credit_card,

        -- Delivery
        o.days_to_deliver,
        o.delivery_delta_days,
        o.delivered_on_time,
        o.hours_to_approve,

        -- Review
        o.review_score,
        o.has_review_comment,
        o.review_created_at,

        -- Date parts for easy slicing
        date_trunc('month', o.purchased_at) as order_month,
        date_trunc('week', o.purchased_at)  as order_week,
        dayofweek(o.purchased_at)           as order_day_of_week

    from orders o
    left join customers c using (customer_id)
    left join dim d using (customer_unique_id)
    left join items i using (order_id)
)

select * from fct
