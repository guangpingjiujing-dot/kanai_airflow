{{ config(materialized='view') }}

with source as (
    select * from {{ source('ecommerce_system', 'payments') }}
),

renamed as (
    select
        payment_id,
        order_id,
        amount as payment_amount,
        payment_date,
        method as payment_method
    from source
)

select * from renamed

