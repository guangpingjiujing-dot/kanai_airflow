{{ config(materialized='table') }}

with fct_orders as (
    select * from {{ ref('fct_orders') }}
),

dim_products as (
    select * from {{ ref('dim_products') }}
),

product_sales as (
    select
        fo.product_id,
        dp.product_name,
        dp.category_id,
        dp.category_name,
        dp.current_price,
        count(distinct fo.order_id) as order_count,
        count(distinct fo.customer_id) as customer_count,
        sum(fo.quantity) as total_quantity_sold,
        sum(fo.line_total) as total_revenue,
        avg(fo.unit_price) as avg_selling_price,
        min(fo.unit_price) as min_selling_price,
        max(fo.unit_price) as max_selling_price,
        min(fo.order_date) as first_sale_date,
        max(fo.order_date) as last_sale_date
    from fct_orders fo
    inner join dim_products dp
        on fo.product_id = dp.product_id
    where fo.order_status != 'cancelled'
    group by
        fo.product_id,
        dp.product_name,
        dp.category_id,
        dp.category_name,
        dp.current_price
),

final as (
    select
        ps.*,
        case
            when ps.order_count = 0 then '未販売'
            when ps.total_quantity_sold < 3 then '低販売'
            when ps.total_quantity_sold < 5 then '中販売'
            else '高販売'
        end as sales_category,
        case
            when ps.current_price < ps.avg_selling_price then '値下げ'
            when ps.current_price > ps.avg_selling_price then '値上げ'
            else '変更なし'
        end as price_trend
    from product_sales ps
)

select * from final

