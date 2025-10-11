{{ config(
    schema='stg',
    materialized='view'
) }}

select
    item.address as token_address,
    timestamp_seconds(item.unixTime) as price_timestamp,
    safe_cast(item.c as numeric) as close,
    safe_cast(item.h as numeric) as high,
    safe_cast(item.o as numeric) as open,
    safe_cast(item.l as numeric) as low,
    safe_cast(item.v as numeric) as volume

from
    {{ source('raw', 'raw_birdeye_ohlcv') }},
    unnest(data.items) as item

where
    safe_cast(item.c as numeric) is not null
    and item.address is not null 
    and item.unixTime is not null