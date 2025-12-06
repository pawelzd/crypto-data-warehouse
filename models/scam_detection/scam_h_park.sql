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
  close_vol_ratio,
  green_share,
  CASE WHEN
    max_to_min_ratio >= 8                    -- large upward move
    AND close_vol_ratio <= 0.05              -- then flat
    AND green_share >= 0.60                  -- mostly upwards candles
  THEN 1 ELSE 0 END AS pattern_pump_and_park
FROM f
WHERE
  max_to_min_ratio >= 8
  AND close_vol_ratio <= 0.05
  AND green_share >= 0.60