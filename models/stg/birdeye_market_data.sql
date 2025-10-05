SELECT 
token_address,
market_cap_usd,
fdv_usd,
total_supply,
liquidity,
circulating_supply
FROM
    {{ source('raw', 'raw_birdeye_market_data') }}
WHERE market_cap_usd IS NOT NULL
AND token_address IS NOT NULL