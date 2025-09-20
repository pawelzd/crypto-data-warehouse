with cleaned as (
    select DISTINCT
        token_address,
        case
            when price_timestamp > 1e14 then timestamp_millis(cast(price_timestamp/1000 as int64))  -- µs → ms
            when price_timestamp > 1e12 then timestamp_millis(cast(price_timestamp as int64))       -- ms
            when price_timestamp > 1e9  then timestamp_millis(cast(price_timestamp*1000 as int64))  -- sec → ms
            else null
        end as price_timestamp,
        safe_cast(price_usd as numeric) as price_usd
    from {{ source('solana_raw_prices', 'raw_bitcoin_1min_prices') }}
    where safe_cast(price_usd as numeric) is not null
      and token_address is not null
      and price_timestamp is not null
)

select * from cleaned
