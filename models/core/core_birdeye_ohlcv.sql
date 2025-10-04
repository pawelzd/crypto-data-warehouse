{{ config(
    schema='core',
    materialized='table'
) }}


with source_data as (
    select * from {{ ref('stg_birdeye_ohlcv') }}
)

select
    s.token_address,
    s.price_timestamp,
    s.price_usd, 
    s.volume
    
from source_data as s

{% if is_incremental() %}

left join (
    select
        token_address,
        max(price_timestamp) as max_timestamp
    from {{ this }}
    group by token_address
) as dest
on s.token_address = dest.token_address

where dest.token_address is null
   or s.price_timestamp > dest.max_timestamp

{% endif %}