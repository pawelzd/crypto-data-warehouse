{% macro rl_prod_hourly_bars_sql(history_hours=none) %}

-- Bounded hourly grid for live inference. The timestamp predicate is kept on
-- the bare token_ohlcv partition column so BigQuery can prune all older days.
WITH revision_override AS (
  SELECT token_address, price_timestamp, price, volume
  FROM {{ source('rl_prod_artifacts', 'ohlcv_revision_override_v1') }}
),

metadata AS (
  SELECT
    token_address,
    MAX(SAFE_CAST(circulating_supply AS FLOAT64)) AS circulating_supply,
    MAX(SAFE_CAST(market_cap_usd AS FLOAT64)) AS market_cap_usd
  FROM {{ ref('birdeye_market_data') }}
  GROUP BY token_address
),

observed AS (
  SELECT
    p.token_address,
    TIMESTAMP_TRUNC(p.price_timestamp, HOUR) AS price_timestamp,
    AVG(SAFE_CAST(p.close AS FLOAT64)) AS price,
    MAX(SAFE_CAST(p.volume AS FLOAT64)) AS volume
  FROM {{ ref('token_ohlcv') }} p
  LEFT JOIN metadata m
    ON p.token_address = m.token_address
  WHERE
  {% if history_hours is not none %}
    p.price_timestamp >= TIMESTAMP_SUB(
    TIMESTAMP_TRUNC(CURRENT_TIMESTAMP(), HOUR),
    INTERVAL {{ history_hours }} HOUR
  )
    AND
  {% endif %}
    SAFE_CAST(p.close AS FLOAT64) > 0
    AND (
      p.token_address = 'btcusdt'
      OR (p.chain = 'sol' AND m.token_address IS NOT NULL)
    )
  GROUP BY p.token_address, price_timestamp
),

bounds AS (
  SELECT
    token_address,
    MIN(price_timestamp) AS start_timestamp,
    MAX(price_timestamp) AS end_timestamp
  FROM observed
  GROUP BY token_address
),

hour_grid AS (
  SELECT
    b.token_address,
    price_timestamp
  FROM bounds b,
  UNNEST(GENERATE_TIMESTAMP_ARRAY(
    b.start_timestamp,
    b.end_timestamp,
    INTERVAL 1 HOUR
  )) AS price_timestamp
),

filled AS (
  SELECT
    g.token_address,
    g.price_timestamp,
    LAST_VALUE(o.price IGNORE NULLS) OVER (
      PARTITION BY g.token_address
      ORDER BY g.price_timestamp
      ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) AS price,
    COALESCE(o.volume, 0.0) AS volume
  FROM hour_grid g
  LEFT JOIN observed o
    USING (token_address, price_timestamp)
),

reconciled AS (
  SELECT
    f.token_address,
    f.price_timestamp,
    COALESCE(r.price, f.price) AS price,
    COALESCE(r.volume, f.volume) AS volume
  FROM filled f
  LEFT JOIN revision_override r
    USING (token_address, price_timestamp)
)

SELECT
  f.token_address,
  f.price_timestamp,
  f.price,
  f.volume,
  CASE
    WHEN f.token_address = 'btcusdt' THEN NULL
    ELSE COALESCE(f.price * m.circulating_supply, m.market_cap_usd)
  END AS mktcap
FROM reconciled f
LEFT JOIN metadata m
  ON f.token_address = m.token_address
WHERE f.price IS NOT NULL
{% endmacro %}
