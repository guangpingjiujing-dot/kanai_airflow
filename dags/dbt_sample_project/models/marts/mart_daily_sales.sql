{{ config(materialized='table') }}

with fct_orders as (
    select * from {{ ref('fct_orders') }}
),

fct_payments as (
    select * from {{ ref('fct_payments') }}
),

daily_order_summary as (
    select
        cast(order_date as date) as order_date,
        count(distinct order_id) as order_count,
        count(distinct customer_id) as customer_count,
        count(distinct product_id) as product_count,
        sum(quantity) as total_quantity,
        sum(line_total) as total_revenue,
        count(distinct case when order_status = 'shipped' then order_id end) as shipped_orders,
        count(distinct case when order_status = 'cancelled' then order_id end) as cancelled_orders,
        count(distinct case when order_status = 'processing' then order_id end) as processing_orders
    from fct_orders
    group by cast(order_date as date)
),

daily_payments as (
    select
        cast(payment_date as date) as payment_date,
        count(distinct payment_id) as payment_count,
        count(distinct order_id) as paid_order_count,
        sum(payment_amount) as total_paid
    from fct_payments
    group by cast(payment_date as date)
),

final as (
    select
        dos.order_date,
        dos.order_count,
        dos.customer_count,
        dos.product_count,
        dos.total_quantity,
        dos.total_revenue,
        dos.shipped_orders,
        dos.cancelled_orders,
        dos.processing_orders,
        coalesce(dp.payment_count, 0) as payment_count,
        coalesce(dp.paid_order_count, 0) as paid_order_count,
        coalesce(dp.total_paid, 0) as total_paid,
        dos.total_revenue - coalesce(dp.total_paid, 0) as unpaid_amount,
        case
            when dos.order_count = 0 then '休業日'
            when dos.order_count < 3 then '低売上日'
            when dos.order_count < 5 then '中売上日'
            else '高売上日'
        end as sales_day_category
    from daily_order_summary dos
    left join daily_payments dp
        on dos.order_date = dp.payment_date
)

select * from final

