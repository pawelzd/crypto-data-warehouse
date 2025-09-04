{{ config(
    materialized='table'
)}}
with source_data as (
    select * from {{ ref('fct__token_prices') }}
)

select DISTINCT    
    s.token_address,
from source_data as s


