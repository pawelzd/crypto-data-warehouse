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
  big_move_share,
  p01_close,
  max_close,
  CASE WHEN
    max_to_min_ratio >= 15                 -- 15x+ range
    AND big_move_share >= 0.02             -- at least some 30%+ moves
    AND SAFE_DIVIDE(p01_close, max_close) <= 0.20  -- closes end near bottom
  THEN 1 ELSE 0 END AS pattern_pump_cliff
FROM f
WHERE
  max_to_min_ratio >= 15
  AND big_move_share >= 0.02
  AND SAFE_DIVIDE(p01_close, max_close) <= 0.20
