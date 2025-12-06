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
  p01_low_ratio,
  p01_close,
  p99_close,
  CASE WHEN
    p01_low_ratio <= 0.10                      -- lows near zero
    AND SAFE_DIVIDE(p01_close, p99_close) >= 0.40  -- but closes not completely wrecked
  THEN 1 ELSE 0 END AS pattern_flash_crash
FROM f
WHERE
  p01_low_ratio <= 0.10
  AND SAFE_DIVIDE(p01_close, p99_close) >= 0.40
