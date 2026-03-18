-- Aggregate multiple payment rows (split payments) to one row per order.
with source as (
    select * from {{ source('olist', 'order_payments') }}
),

aggregated as (
    select
        order_id,
        count(*)                                                as payment_installments_total,
        round(sum(payment_value), 2)                            as total_payment_value,
        -- predominant payment type (highest value)
        arg_max(payment_type, payment_value)                    as primary_payment_type,
        bool_or(payment_type = 'voucher')                       as used_voucher,
        bool_or(payment_type = 'boleto')                        as used_boleto,
        bool_or(payment_type = 'credit_card')                   as used_credit_card,
        bool_or(payment_type = 'debit_card')                    as used_debit_card
    from source
    group by order_id
)

select * from aggregated
