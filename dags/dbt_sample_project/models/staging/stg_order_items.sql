{{ config(materialized='view') }}

with source as (
    select * from {{ source('ecommerce_system', 'order_items') }}
),

renamed as (
    select
        order_id,
        product_id,
        quantity,
        unit_price,
        quantity * unit_price as line_total
    from source
)

select * from renamed

