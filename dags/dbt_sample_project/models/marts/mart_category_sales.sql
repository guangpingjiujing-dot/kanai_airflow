{{ config(materialized='table') }}

with fct_orders as (
    select * from {{ ref('fct_orders') }}
),

dim_products as (
    select * from {{ ref('dim_products') }}
),

dim_categories as (
    select * from {{ ref('dim_categories') }}
),

orders_with_categories as (
    select
        fo.order_id,
        fo.order_date,
        fo.order_status,
        fo.product_id,
        dp.category_id,
        dc.category_name,
        fo.quantity,
        fo.line_total
    from fct_orders fo
    inner join dim_products dp
        on fo.product_id = dp.product_id
    inner join dim_categories dc
        on dp.category_id = dc.category_id
    where fo.order_status != 'cancelled'
),

category_sales as (
    select
        category_id,
        category_name,
        count(distinct order_id) as order_count,
        count(distinct product_id) as product_count,
        sum(quantity) as total_quantity_sold,
        sum(line_total) as total_revenue,
        avg(line_total) as avg_order_value
    from orders_with_categories
    group by category_id, category_name
),

final as (
    select
        cs.*,
        case
            when cs.total_revenue = 0 then '未販売'
            when cs.total_revenue < 50000 then '低売上'
            when cs.total_revenue < 100000 then '中売上'
            else '高売上'
        end as revenue_category
    from category_sales cs
)

select * from final

