{{ config(
    materialized='materialized_view'
) }}
with source_data as (
    select * from {{ ref('fct__token_prices') }}
)

select DISTINCT    
    s.token_address,
    s.price_timestamp,
    s.price_usd
    
from source_data as s


