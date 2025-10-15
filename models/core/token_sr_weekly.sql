WITH weekly AS (
  SELECT
    token_address,
    EXTRACT(YEAR FROM price_timestamp) AS year,
    EXTRACT(ISOWEEK FROM price_timestamp) AS week,
    MIN(low) AS weekly_low,
    MAX(high) AS weekly_high,
    FIRST_VALUE(open) OVER (PARTITION BY token_address, EXTRACT(YEAR FROM price_timestamp), EXTRACT(ISOWEEK FROM price_timestamp) ORDER BY price_timestamp) AS weekly_open,
    LAST_VALUE(close) OVER (PARTITION BY token_address, EXTRACT(YEAR FROM price_timestamp), EXTRACT(ISOWEEK FROM price_timestamp) ORDER BY price_timestamp 
      ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS weekly_close
  FROM {{ ref('token_ohlcv') }}
  GROUP BY
    token_address, year, week
)

SELECT
  token_address,
  year,
  week,
  weekly_high,
  weekly_low,
  weekly_open,
  weekly_close,
  (weekly_high + weekly_low + weekly_close)/3 AS pivot,
  (2 * ((weekly_high + weekly_low + weekly_close)/3) - weekly_low) AS r1,
  (2 * ((weekly_high + weekly_low + weekly_close)/3) - weekly_high) AS s1,
  ((weekly_high + weekly_low + weekly_close)/3) + (weekly_high - weekly_low) AS r2,
  ((weekly_high + weekly_low + weekly_close)/3) - (weekly_high - weekly_low) AS s2
FROM weekly
ORDER BY token_address, year DESC, week DESC;