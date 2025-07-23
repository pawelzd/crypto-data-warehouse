with source as (
    select * from {{ source('solana_raw', 'tokens') }}
),
cleaned as (
    select
        address as token_address,
        symbol as token_symbol,
        name as token_name,
        safe_cast(decimals as int64) as token_decimals,
        logoURI,
        -- coingecko_id as coingecko_id,
        daily_volume,
        created_at,
        freeze_authority,
        mint_authority as token_authority_address,
        permanent_delegate as permanent_delegate_address,
        minted_at
    from source
),
final as (
    select
        {{ dbt_utils.generate_surrogate_key(['token_address']) }} as token_metadata_sk, 
        *,
    from cleaned
    where
        token_address is not null
        and token_name is not null
)
select * from final