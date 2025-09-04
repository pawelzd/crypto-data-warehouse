select
    wallet as wallet_address,
    mint as token_address,
    safe_cast(first_acquired_date as timestamp) as first_acquired_timestamp,
    safe_cast(last_acquired_date as timestamp) as last_acquired_timestamp,
    safe_cast(no_transactions as int64) as transaction_count,
    total_held_last as asset_amount,
    tx_id_min as first_transaction_id,
    tx_id_max as last_transaction_id

from {{ source('solana_raw', 'wallet_tokens') }}

where 
    mint is not null 
    and mint != ''
    and wallet is not null
    -- and length(mint) between 32 and 44