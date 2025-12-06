{{ config(materialized='view') }}

WITH f AS (
  SELECT *
  FROM {{ ref('scam_h_features') }}
)

SELECT
  chain,
  token_address,
  n,
  last_ts,
  max_to_min_ratio,
  green_share,
  p01_close,
  max_close,
  CASE WHEN
    max_to_min_ratio >= 10                     -- big range
    AND green_share <= 0.35                    -- mostly red candles
    AND SAFE_DIVIDE(p01_close, max_close) <= 0.15
  THEN 1 ELSE 0 END AS pattern_decay_to_dust
FROM f
WHERE
  max_to_min_ratio >= 10
  AND green_share <= 0.35
  AND SAFE_DIVIDE(p01_close, max_close) <= 0.15
