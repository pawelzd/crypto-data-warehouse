WITH base AS (
  SELECT
    t.token_address,
    DATE(t.price_timestamp) AS day,
    t.low,
    t.high,
    t.open,
    t.close,
    t.price_timestamp
  FROM {{ ref('token_ohlcv') }} t
),
daily AS (
  SELECT
    token_address,
    day,
    MIN(low)  AS daily_low,
    MAX(high) AS daily_high,
    (ARRAY_AGG(open  ORDER BY price_timestamp ASC  LIMIT 1))[OFFSET(0)] AS daily_open,
    (ARRAY_AGG(close ORDER BY price_timestamp DESC LIMIT 1))[OFFSET(0)] AS daily_close
  FROM base
  GROUP BY token_address, day
)
SELECT
  token_address,
  day,
  daily_high,
  daily_low,
  daily_open,
  daily_close,
  (daily_high + daily_low + daily_close) / 3 AS pivot,
  (2 * ((daily_high + daily_low + daily_close) / 3) - daily_low)  AS r1,
  (2 * ((daily_high + daily_low + daily_close) / 3) - daily_high) AS s1,
  ((daily_high + daily_low + daily_close) / 3) + (daily_high - daily_low) AS r2,
  ((daily_high + daily_low + daily_close) / 3) - (daily_high - daily_low) AS s2
FROM daily
ORDER BY token_address, day DESC
