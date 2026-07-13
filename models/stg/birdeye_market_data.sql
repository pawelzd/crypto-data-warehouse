WITH streamed AS (
  SELECT
    address AS token_address,
    market_cap AS market_cap_usd,
    fdv AS fdv_usd,
    total_supply,
    liquidity,
    circulating_supply,
    'sol' AS chain
  FROM {{ source('streamed_datapublic', 'public_tokens_to_monitor') }}
  WHERE market_cap IS NOT NULL
    AND address IS NOT NULL
),

raw_backfill AS (
  SELECT
    address AS token_address,
    market_cap AS market_cap_usd,
    fdv AS fdv_usd,
    total_supply,
    liquidity,
    circulating_supply,
    chain
  FROM {{ source('raw', 'raw_birdeye_market_data') }}
  WHERE market_cap IS NOT NULL
    AND address IS NOT NULL
)

SELECT *
FROM streamed

UNION ALL

SELECT r.*
FROM raw_backfill r
LEFT JOIN streamed s
  USING (token_address)
WHERE s.token_address IS NULL
