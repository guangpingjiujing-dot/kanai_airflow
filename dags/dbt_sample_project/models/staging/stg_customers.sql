{{ config(materialized='view') }}

with source as (
    select * from {{ source('ecommerce_system', 'customers') }}
),

renamed as (
    select
        customer_id,
        full_name,
        email,
        city,
        created_at as registered_at
    from source
)

select * from renamed

