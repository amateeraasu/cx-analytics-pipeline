with source as (
    select * from {{ source('olist', 'order_items') }}
),

renamed as (
    select
        {{ dbt_utils.generate_surrogate_key(['order_id', 'order_item_id']) }} as order_item_sk,
        order_id,
        order_item_id,
        product_id,
        seller_id,
        shipping_limit_date::TIMESTAMP as shipping_limit_at,
        price,
        freight_value
    from source
)

select * from renamed
