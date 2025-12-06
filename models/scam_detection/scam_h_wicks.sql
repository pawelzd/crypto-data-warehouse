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
  wickiness_share,
  avg_body_frac,
  range_ratio,
  CASE WHEN
    wickiness_share >= 0.70                   -- 70%+ wick-heavy candles
    AND avg_body_frac <= 0.20                 -- tiny bodies
    AND range_ratio >= 0.30                   -- big intrabar spans
  THEN 1 ELSE 0 END AS pattern_extreme_wick
FROM f
WHERE
  wickiness_share >= 0.70
  AND avg_body_frac <= 0.20
  AND range_ratio >= 0.30