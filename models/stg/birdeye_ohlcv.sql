{{ config(
    schema='stg',
    materialized='view'
) }}

select
    item.address as token_address,
    timestamp_seconds(item.unixTime) as price_timestamp,
    safe_cast(item.c as numeric) as price_usd,
    safe_cast(item.v as numeric) as volume

from
    {{ source('raw', 'raw_birdeye_ohlcv') }},
    unnest(data.items) as item

where
    safe_cast(item.c as numeric) is not null
    and item.address is not null 
    and item.unixTime is not null