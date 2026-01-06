{{ config(materialized='table') }}

with payments as (
    select * from {{ ref('stg_payments') }}
)

select * from payments

