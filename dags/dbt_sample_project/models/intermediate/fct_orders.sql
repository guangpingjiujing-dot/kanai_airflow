{{ config(materialized='table') }}

with orders as (
    select * from {{ ref('stg_orders') }}
),

order_items as (
    select * from {{ ref('stg_order_items') }}
),

final as (
    select
        o.order_id,
        o.customer_id,
        o.order_date,
        o.order_status,
        oi.product_id,
        oi.quantity,
        oi.unit_price,
        oi.line_total
    from orders o
    inner join order_items oi
        on o.order_id = oi.order_id
)

select * from final

