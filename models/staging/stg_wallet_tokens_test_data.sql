select
    wallet as wallet_address,
    mint as token_address,
    -- safe_cast(split(load_id, '__')[offset(1)] as timestamp) as load_date,
    safe_cast(first_acquired_date as timestamp) as first_acquired_timestamp,
    safe_cast(last_acquired_date as timestamp) as last_acquired_timestamp,
    safe_cast(no_transactions as int64) as transaction_count,
    safe_cast(total_held_last as numeric) as asset_amount,
    tx_id_min,
    tx_id_max,

from {{ source('solana_raw', 'wallet_tokens') }}

where 
    mint is not null 
    and mint != ''
    and wallet is not null