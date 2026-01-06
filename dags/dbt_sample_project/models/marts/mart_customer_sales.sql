{{ config(materialized='table') }}

with fct_orders as (
    select * from {{ ref('fct_orders') }}
),

dim_customers as (
    select * from {{ ref('dim_customers') }}
),

fct_payments as (
    select * from {{ ref('fct_payments') }}
),

customer_order_summary as (
    select
        customer_id,
        count(distinct order_id) as total_orders,
        count(distinct case when order_status = 'shipped' then order_id end) as shipped_orders,
        count(distinct case when order_status = 'cancelled' then order_id end) as cancelled_orders,
        count(distinct case when order_status = 'processing' then order_id end) as processing_orders,
        sum(quantity) as total_quantity_purchased,
        sum(line_total) as total_spent,
        min(order_date) as first_order_date,
        max(order_date) as last_order_date
    from fct_orders
    group by customer_id
),

customer_payments as (
    select
        fo.customer_id,
        sum(fp.payment_amount) as total_paid
    from fct_orders fo
    inner join fct_payments fp
        on fo.order_id = fp.order_id
    group by fo.customer_id
),

final as (
    select
        dc.customer_id,
        dc.full_name,
        dc.email,
        dc.city,
        dc.registered_at,
        coalesce(cos.total_orders, 0) as total_orders,
        coalesce(cos.shipped_orders, 0) as shipped_orders,
        coalesce(cos.cancelled_orders, 0) as cancelled_orders,
        coalesce(cos.processing_orders, 0) as processing_orders,
        coalesce(cos.total_quantity_purchased, 0) as total_quantity_purchased,
        coalesce(cos.total_spent, 0) as total_spent,
        coalesce(cp.total_paid, 0) as total_paid,
        cos.total_spent - coalesce(cp.total_paid, 0) as unpaid_amount,
        cos.first_order_date,
        cos.last_order_date,
        case
            when cos.total_orders = 0 then '新規顧客'
            when cos.total_orders = 1 then 'リピーター（1回）'
            when cos.total_orders >= 2 and cos.total_orders < 5 then 'リピーター（2-4回）'
            else 'VIP顧客（5回以上）'
        end as customer_segment,
        case
            when cos.total_spent = 0 then '未購入'
            when cos.total_spent < 20000 then '低価値顧客'
            when cos.total_spent < 50000 then '中価値顧客'
            else '高価値顧客'
        end as customer_value_segment
    from dim_customers dc
    left join customer_order_summary cos
        on dc.customer_id = cos.customer_id
    left join customer_payments cp
        on dc.customer_id = cp.customer_id
)

select * from final

