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
  green_share,
  max_to_min_ratio,
  CASE WHEN
    (green_share >= 0.85 OR green_share <= 0.15)   -- almost only one direction
    AND max_to_min_ratio >= 5                      -- strong move
  THEN 1 ELSE 0 END AS pattern_liquidity_trap
FROM f
WHERE
  (green_share >= 0.85 OR green_share <= 0.15)
  AND max_to_min_ratio >= 5