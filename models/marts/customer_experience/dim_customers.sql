with customer_orders as (
    select * from {{ ref('int_customer_orders') }}
),

geo as (
    -- Aggregate to city/state grain to avoid fanout on the join below.
    select
        city,
        state,
        round(avg(lat), 4) as lat,
        round(avg(lng), 4) as lng
    from {{ ref('stg_geolocation') }}
    group by city, state
),

customers as (
    select
        {{ dbt_utils.generate_surrogate_key(['customer_unique_id']) }} as customer_sk,
        co.customer_unique_id,
        co.city,
        co.state,
        g.lat,
        g.lng,

        co.total_orders,
        co.delivered_orders,
        co.canceled_orders,
        co.first_order_at,
        co.last_order_at,
        co.customer_lifespan_days,
        co.total_spend_brl,
        co.avg_order_value_brl,
        co.avg_review_score,
        co.review_count,
        co.avg_days_to_deliver,
        co.on_time_deliveries,

        -- Segments
        case
            when co.total_orders = 1 then 'one_time'
            when co.total_orders between 2 and 4 then 'repeat'
            else 'loyal'
        end as order_frequency_segment,

        case
            when co.avg_review_score >= 4 then 'satisfied'
            when co.avg_review_score >= 3 then 'neutral'
            else 'dissatisfied'
        end as satisfaction_segment

    from customer_orders co
    left join geo g on co.city = g.city and co.state = g.state
)

select * from customers
