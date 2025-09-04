
WITH w AS (
  SELECT
    token_address,
    first_acquired_timestamp,
    ANY_VALUE(price_usd_on_acquisition) AS acq_price,
    -- max price in [t_acq, t_acq + 7 days]
    MAX(price_usd) AS max_price_7d
  FROM {{ ref('ml_tokens_prices_filtered_7daysafter') }}
  GROUP BY token_address, first_acquired_timestamp
)
SELECT
  token_address,
  first_acquired_timestamp,
  -- 1 if peak >= 30% over acquisition price, else 0
  IF(
    SAFE_DIVIDE(max_price_7d - acq_price, acq_price) >= 0.25,
    1, 0
  ) AS label_peak_30pct_7d,
  -- (optional) diagnostics
  acq_price,
  max_price_7d,
  SAFE_DIVIDE(max_price_7d - acq_price, acq_price) AS pct_gain_7d
FROM w
