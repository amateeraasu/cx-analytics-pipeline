with source as (
    select * from {{ source('olist', 'products') }}
),

translation as (
    select * from {{ source('olist', 'category_translation') }}
),

renamed as (
    select
        p.product_id,
        coalesce(t.product_category_name_english, p.product_category_name) as category_name_en,
        p.product_category_name                                             as category_name_pt,
        p.product_name_lenght                                               as product_name_length,
        p.product_description_lenght                                        as product_description_length,
        p.product_photos_qty                                                as photos_qty,
        p.product_weight_g                                                  as weight_g,
        p.product_length_cm                                                 as length_cm,
        p.product_height_cm                                                 as height_cm,
        p.product_width_cm                                                  as width_cm
    from source p
    left join translation t using (product_category_name)
)

select * from renamed
