-- One representative lat/lon per zip code prefix (median point).
with source as (
    select * from {{ source('olist', 'geolocation') }}
),

deduped as (
    select
        geolocation_zip_code_prefix         as zip_code_prefix,
        round(avg(geolocation_lat), 4)      as lat,
        round(avg(geolocation_lng), 4)      as lng,
        any_value(geolocation_city)         as city,
        any_value(geolocation_state)        as state
    from source
    group by geolocation_zip_code_prefix
)

select * from deduped
