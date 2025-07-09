select
    wallet as wallet_address,
    mint as token_address,
    load_id,
    safe_cast(split(load_id, '__')[offset(1)] as date) as load_date,
    safe_cast(first_acquired_date as timestamp) as first_acquired_timestamp,
    safe_cast(no_transactions as int64) as transaction_count,
    safe_cast(total as numeric) as asset_amount,


from {{ source('solana_raw', 'raw_solana_wallet_history') }}

where 
    mint is not null 
    and mint != ''
    and wallet is not null
    and length(mint) between 32 and 44