{{ config(materialized='view') }}

with source as (
    select * from {{ source('ecommerce_system', 'categories') }}
),

renamed as (
    select
        category_id,
        name as category_name
    from source
)

select * from renamed

