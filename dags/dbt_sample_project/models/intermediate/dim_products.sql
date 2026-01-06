{{ config(materialized='table') }}

with products as (
    select * from {{ ref('stg_products') }}
),

categories as (
    select * from {{ ref('stg_categories') }}
),

final as (
    select
        p.product_id,
        p.product_name,
        p.category_id,
        c.category_name,
        p.price as current_price,
        p.product_created_at
    from products p
    inner join categories c
        on p.category_id = c.category_id
)

select * from final

