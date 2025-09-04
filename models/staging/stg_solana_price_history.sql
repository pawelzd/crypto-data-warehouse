select
    item.address as token_address,
    timestamp_seconds(item.unixTime) as price_timestamp,
    safe_cast(item.value as numeric) as price_usd

from
    {{ source('solana_raw_prices', 'raw_solana_token_prices') }},
    unnest(data.items) as item

where
    safe_cast(item.value as numeric) is not null
    and item.address is not null 
    and item.unixTime is not null