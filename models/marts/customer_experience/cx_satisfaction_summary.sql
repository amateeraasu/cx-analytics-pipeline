-- Monthly CX KPI summary: satisfaction scores, delivery performance, and order volume.
with fct as (
    select * from {{ ref('fct_orders') }}
    where order_status = 'delivered'
),

monthly as (
    select
        order_month,

        count(distinct order_id)                                    as total_orders,
        round(avg(review_score), 3)                                 as avg_review_score,
        round(avg(days_to_deliver), 2)                              as avg_days_to_deliver,

        round(
            cast(count(case when review_score >= 4 then 1 end) as double)
            / nullif(count(review_score), 0), 4
        )                                                           as csat_rate,

        round(
            cast(count(case when delivered_on_time then 1 end) as double)
            / nullif(count(order_id), 0), 4
        )                                                           as on_time_rate,

        round(avg(total_payment_value), 2)                          as avg_order_value_brl,
        round(sum(total_payment_value), 2)                          as total_gmv_brl,

        count(case when has_review_comment then 1 end)              as orders_with_comment,
        count(case when review_score = 1 then 1 end)                as low_score_orders,
        count(case when used_voucher then 1 end)                    as voucher_orders

    from fct
    group by order_month
)

select * from monthly
order by order_month
