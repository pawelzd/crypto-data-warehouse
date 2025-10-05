SELECT 
token_address,
market_cap_usd,
total_supply         
FROM
    {{ ref('birdeye_market_data') }}
