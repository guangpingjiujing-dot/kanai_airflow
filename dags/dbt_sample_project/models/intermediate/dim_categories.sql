{{ config(materialized='table') }}

with categories as (
    select * from {{ ref('stg_categories') }}
)

select * from categories

