-- CREATE OR REPLACE TABLE `positive-tuner-255507.silver.bitcoin_hourly_prices` AS
WITH cleaned AS (
  SELECT DISTINCT 
    token_address,
    price_timestamp,
    price_usd
  FROM {{ ref('stg_bitcoin_1min_price_history') }} 
),

hourly AS (
  SELECT
    token_address,
    TIMESTAMP_TRUNC(price_timestamp, HOUR) AS hour_start,         
    ARRAY_AGG(STRUCT(price_timestamp, price_usd)
              ORDER BY price_timestamp DESC LIMIT 1)[OFFSET(0)].price_usd AS close_price
  FROM cleaned 
  WHERE price_timestamp >= TIMESTAMP('2022-01-01 00:00:00+00')
  GROUP BY token_address, hour_start
)

SELECT
  token_address,
  hour_start AS price_timestamp,
  close_price AS price_usd
FROM hourly
ORDER BY token_address, price_timestamp
