select
    address as token_address,
    timestamp_seconds(unixTime) as price_timestamp,
    safe_cast(value as numeric) as price_usd

from {{ source('solana_raw', 'solana_price_history') }}


where
    safe_cast(value as numeric) is not null
    and address is not null
    and unixTime is not null
    and value is not null