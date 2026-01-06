{{ config(materialized='view') }}

with source as (
    select * from {{ source('ecommerce_system', 'products') }}
),

renamed as (
    select
        product_id,
        name as product_name,
        category_id,
        price,
        created_at as product_created_at
    from source
)

select * from renamed

