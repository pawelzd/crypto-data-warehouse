with source as (
    select * from {{ source('solana_raw', 'solana_mint_metadata') }}
),
cleaned as (
    select
        address as token_address,
        symbol as token_symbol,
        name as token_name,
        safe_cast(decimals as int64) as token_decimals,
        logo_uri,
        extensions_coingecko_id as coingecko_id,
        extensions_website as website_url,
        extensions_twitter as twitter_handle,
        extensions_discord as discord_url,
        safe_cast(extensions_medium as string) as medium_url
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
        -- and length(token_address) between 32 and 44
)
select * from final