
  SELECT DISTINCT
    CAST(address AS STRING) AS token_address,
    SAFE_CAST(price AS NUMERIC) AS price_usd,
    SAFE_CAST(liquidity AS NUMERIC) AS liquidity,
    SAFE_CAST(total_supply AS NUMERIC) AS total_supply,
    SAFE_CAST(circulating_supply AS NUMERIC) AS circulating_supply,
    SAFE_CAST(fdv AS NUMERIC) AS fdv_usd,
    SAFE_CAST(market_cap AS NUMERIC) AS market_cap_usd,
    SAFE_CAST(is_scaled_ui_token AS BOOL) AS is_scaled_ui_token,
    SAFE_CAST(multiplier AS NUMERIC) AS multiplier
  FROM {{ source('solana_raw_prices', 'raw_tokens_metadata') }}

