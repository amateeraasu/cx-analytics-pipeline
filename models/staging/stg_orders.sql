with source as (
    select * from {{ source('olist', 'orders') }}
),

renamed as (
    select
        order_id,
        customer_id,
        order_status,
        order_purchase_timestamp::TIMESTAMP     as purchased_at,
        order_approved_at::TIMESTAMP            as approved_at,
        order_delivered_carrier_date::TIMESTAMP as carrier_delivered_at,
        order_delivered_customer_date::TIMESTAMP as customer_delivered_at,
        order_estimated_delivery_date::TIMESTAMP as estimated_delivery_at
    from source
)

select * from renamed
