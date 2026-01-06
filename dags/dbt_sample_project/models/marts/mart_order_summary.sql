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

order_aggregated as (
    select
        order_id,
        customer_id,
        min(order_date) as order_date,
        min(order_status) as order_status,
        count(distinct product_id) as product_count,
        sum(quantity) as total_quantity,
        sum(line_total) as order_total
    from fct_orders
    group by order_id, customer_id
),

order_payments as (
    select
        order_id,
        sum(payment_amount) as total_paid,
        count(distinct payment_id) as payment_count,
        min(payment_date) as first_payment_date,
        max(payment_date) as last_payment_date,
        string_agg(payment_method, ', ') within group (order by payment_method) as payment_methods
    from (
        select distinct order_id, payment_id, payment_amount, payment_date, payment_method
        from fct_payments
    ) distinct_payments
    group by order_id
),

final as (
    select
        oa.order_id,
        oa.customer_id,
        dc.full_name as customer_name,
        dc.city as customer_city,
        oa.order_date,
        oa.order_status,
        oa.product_count,
        oa.total_quantity,
        oa.order_total,
        coalesce(op.total_paid, 0) as total_paid,
        op.payment_count,
        oa.order_total - coalesce(op.total_paid, 0) as unpaid_amount,
        op.first_payment_date,
        op.last_payment_date,
        op.payment_methods,
        case
            when op.total_paid is null then '未払い'
            when op.total_paid < oa.order_total then '一部支払い'
            when op.total_paid = oa.order_total then '支払い済み'
            else '過払い'
        end as payment_status,
        case
            when oa.order_status = 'cancelled' then 'キャンセル'
            when oa.order_status = 'shipped' then '出荷済み'
            when oa.order_status = 'processing' then '処理中'
            else oa.order_status
        end as order_status_jp
    from order_aggregated oa
    inner join dim_customers dc
        on oa.customer_id = dc.customer_id
    left join order_payments op
        on oa.order_id = op.order_id
)

select * from final

