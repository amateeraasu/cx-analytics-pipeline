-- Joins orders with payments and reviews; computes delivery performance metrics.
with orders as (
    select * from {{ ref('stg_orders') }}
),

payments as (
    select * from {{ ref('stg_order_payments') }}
),

reviews as (
    -- Use the latest review per order if duplicates exist.
    select distinct on (order_id)
        order_id,
        review_score,
        comment_title,
        comment_message,
        review_created_at,
        review_answered_at
    from {{ ref('stg_order_reviews') }}
    order by order_id, review_created_at desc
),

enriched as (
    select
        o.order_id,
        o.customer_id,
        o.order_status,
        o.purchased_at,
        o.approved_at,
        o.carrier_delivered_at,
        o.customer_delivered_at,
        o.estimated_delivery_at,

        -- Delivery performance
        date_diff('day', o.purchased_at, o.customer_delivered_at)          as days_to_deliver,
        date_diff('day', o.estimated_delivery_at, o.customer_delivered_at) as delivery_delta_days,
        o.customer_delivered_at <= o.estimated_delivery_at                 as delivered_on_time,

        -- Approval speed
        date_diff('hour', o.purchased_at, o.approved_at)                   as hours_to_approve,

        -- Payment
        p.total_payment_value,
        p.primary_payment_type,
        p.used_voucher,
        p.used_credit_card,

        -- Review
        r.review_score,
        r.comment_message is not null
            and r.comment_message != ''                                     as has_review_comment,
        r.review_created_at

    from orders o
    left join payments p using (order_id)
    left join reviews r using (order_id)
)

select * from enriched
