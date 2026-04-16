-- Aggregates order history to the customer_unique_id grain.
with orders as (
    select * from {{ ref('int_orders_enriched') }}
),

customers as (
    select * from {{ ref('stg_customers') }}
),

customer_orders as (
    select
        c.customer_unique_id,
        -- A customer may have different customer_ids per order (Olist quirk);
        -- use the most recent state/city as the canonical address.
        {{ arg_max('c.state', 'o.purchased_at') }}                  as state,
        {{ arg_max('c.city', 'o.purchased_at') }}                   as city,

        count(o.order_id)                                           as total_orders,
        count(case when o.order_status = 'delivered' then 1 end)    as delivered_orders,
        count(case when o.order_status = 'canceled' then 1 end)     as canceled_orders,

        min(o.purchased_at)                                         as first_order_at,
        max(o.purchased_at)                                         as last_order_at,
        {{ date_diff_days('min(o.purchased_at)', 'max(o.purchased_at)') }} as customer_lifespan_days,

        round(sum(o.total_payment_value), 2)                        as total_spend_brl,
        round(avg(o.total_payment_value), 2)                        as avg_order_value_brl,

        round(avg(o.review_score), 2)                               as avg_review_score,
        count(o.review_score)                                       as review_count,
        round(avg(o.days_to_deliver), 1)                            as avg_days_to_deliver,
        count(case when o.delivered_on_time then 1 end)             as on_time_deliveries

    from customers c
    left join orders o using (customer_id)
    group by c.customer_unique_id
)

select * from customer_orders
