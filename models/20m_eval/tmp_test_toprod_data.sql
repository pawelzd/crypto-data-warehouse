WITH mcap AS (
  SELECT token_address, circulating_supply AS circSupply
  FROM `stg.birdeye_market_data`

  UNION ALL
  SELECT id AS token_address, circSupply
  FROM `core.jup_tmp_v2`

  UNION ALL
  SELECT id AS token_address, circSupply
  FROM `core.token_metadata_jup_tmp`
),
mcap_dedup AS (
  SELECT
    token_address,
    MAX(circSupply) AS circSupply
  FROM mcap
  GROUP BY token_address
),

-- 1) Identify “eligible” 05:00 anchors (per token, per day)
anchors AS (
  SELECT
    t.token_address,
    t.price_timestamp AS anchor_ts
  FROM `crypto-trading-474111.core.token_ohlcv` t
  JOIN mcap_dedup md
    ON t.token_address = md.token_address
  WHERE t.chain = 'sol'
    AND t.price_timestamp >= TIMESTAMP('2025-04-24')
    -- anchor must be exactly 05:00:00
    AND EXTRACT(HOUR   FROM t.price_timestamp) = 5
    AND EXTRACT(MINUTE FROM t.price_timestamp) = 0
    AND EXTRACT(SECOND FROM t.price_timestamp) = 0
    -- eligibility condition at 05:00
    AND t.close * md.circSupply >= 20000000
)

-- 2) Return the next 24 hours of data starting at each eligible anchor
SELECT
  t.token_address as address,
  t.price_timestamp as hour_ts,
  t.close as price,
  t.volume
FROM `crypto-trading-474111.core.token_ohlcv` t
JOIN anchors a
  ON t.token_address = a.token_address
 AND t.price_timestamp >= a.anchor_ts
 AND t.price_timestamp <  TIMESTAMP_ADD(a.anchor_ts, INTERVAL 24 HOUR)
WHERE t.chain = 'sol'
  AND t.price_timestamp >= TIMESTAMP('2025-05-01') and t.price_timestamp < TIMESTAMP('2025-08-26 15:00:00 UTC')
ORDER BY t.token_address, t.price_timestamp
