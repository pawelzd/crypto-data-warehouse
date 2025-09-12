
WITH w AS (
  SELECT
    tp.token_address,
    tp.first_acquired_timestamp,
    tp.type,
    ANY_VALUE(tp.price_usd_on_acquisition) AS acq_price,
    -- max price in [t_acq, t_acq + 7 days]
    MAX(tp.price_usd) AS max_price_7d
  FROM {{ ref('ml_tokens_prices_filtered_7daysafter') }} AS tp
  WHERE tp.price_usd IS NOT NULL
  GROUP BY tp.token_address, tp.first_acquired_timestamp, tp.type
)
SELECT
  w.token_address,
  w.first_acquired_timestamp,
  -- 1 if peak >= 30% over acquisition price, else 0
  IF(
    SAFE_DIVIDE(w.max_price_7d - w.acq_price, w.acq_price) >= 0.25,
    1, 0
  ) AS label_peak_30pct_7d,
  -- (optional) diagnostics
  w.acq_price,
  w.max_price_7d,
  SAFE_DIVIDE(w.max_price_7d - w.acq_price, w.acq_price) AS pct_gain_7d,
  w.type
FROM w
