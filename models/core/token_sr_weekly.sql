WITH base AS (
  SELECT
    t.token_address,
    t.price_timestamp,
    t.low,
    t.high,
    t.open,
    t.close,
    EXTRACT(ISOYEAR FROM t.price_timestamp) AS year,
    EXTRACT(ISOWEEK FROM t.price_timestamp) AS week
  FROM {{ ref('token_ohlcv') }} t
),
weekly AS (
  SELECT
    token_address,
    year,
    week,
    MIN(low)  AS weekly_low,
    MAX(high) AS weekly_high,
    (ARRAY_AGG(open  ORDER BY price_timestamp ASC  LIMIT 1))[OFFSET(0)] AS weekly_open,
    (ARRAY_AGG(close ORDER BY price_timestamp DESC LIMIT 1))[OFFSET(0)] AS weekly_close
  FROM base
  GROUP BY token_address, year, week
)
SELECT
  token_address,
  year,
  week,
  weekly_high,
  weekly_low,
  weekly_open,
  weekly_close,
  (weekly_high + weekly_low + weekly_close) / 3 AS pivot,
  (2 * ((weekly_high + weekly_low + weekly_close) / 3) - weekly_low)  AS r1,
  (2 * ((weekly_high + weekly_low + weekly_close) / 3) - weekly_high) AS s1,
  ((weekly_high + weekly_low + weekly_close) / 3) + (weekly_high - weekly_low) AS r2,
  ((weekly_high + weekly_low + weekly_close) / 3) - (weekly_high - weekly_low) AS s2
FROM weekly
ORDER BY token_address, year DESC, week DESC
