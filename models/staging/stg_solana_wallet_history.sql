select
    wallet_address,
    mint as token_address,
    load_date as load_date,
    safe_cast(first_acquired_date as timestamp) as first_acquired_timestamp,
    safe_cast(no_transactions as int64) as transaction_count,
    safe_cast(total as numeric) as asset_amount

from {{ source('solana_raw', 'solana_wallet_history_manual') }}

where 
    mint is not null 
    and mint != ''
    and wallet_address is not null
    and length(mint) between 32 and 44