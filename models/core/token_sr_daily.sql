WITH daily AS (
  SELECT
    token_address,
    DATE(price_timestamp) AS day,
    MIN(low) AS daily_low,
    MAX(high) AS daily_high,
    FIRST_VALUE(open) OVER (PARTITION BY token_address, DATE(price_timestamp) ORDER BY price_timestamp) AS daily_open,
    LAST_VALUE(close) OVER (PARTITION BY token_address, DATE(price_timestamp) ORDER BY price_timestamp 
      ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS daily_close
  FROM {{ ref('token_ohlcv') }}
  GROUP BY
    token_address, DATE(price_timestamp)
)

SELECT
  token_address,
  day,
  daily_high,
  daily_low,
  daily_open,
  daily_close,
  (daily_high + daily_low + daily_close)/3 AS pivot,
  (2 * ((daily_high + daily_low + daily_close)/3) - daily_low) AS r1,
  (2 * ((daily_high + daily_low + daily_close)/3) - daily_high) AS s1,
  ((daily_high + daily_low + daily_close)/3) + (daily_high - daily_low) AS r2,
  ((daily_high + daily_low + daily_close)/3) - (daily_high - daily_low) AS s2
FROM daily
ORDER BY token_address, day DESC