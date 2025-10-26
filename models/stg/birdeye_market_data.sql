SELECT 
address AS token_address,
market_cap AS market_cap_usd,
fdv AS fdv_usd,
total_supply,
liquidity,
circulating_supply,
chain
FROM
    {{ source('raw', 'raw_birdeye_market_data') }}
WHERE market_cap IS NOT NULL
AND address IS NOT NULL