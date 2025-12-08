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
  alt_dir_share,
  median_body_frac,
  p05_range,
  p95_range,
  range_ratio,
  close_vol_ratio,
  CASE WHEN
    alt_dir_share >= 0.60                         -- lots of direction flips
    AND median_body_frac <= 0.10                  -- very small bodies vs range
    AND SAFE_DIVIDE(p95_range, NULLIF(p05_range,0)) <= 5  -- compressed intrabar ranges
    AND close_vol_ratio BETWEEN 0.01 AND 0.20     -- not dead flat, not hyper-volatile
  THEN 1 ELSE 0 END AS pattern_microstructure_anomaly
FROM f
WHERE
  alt_dir_share IS NOT NULL
  AND median_body_frac IS NOT NULL
  AND p05_range IS NOT NULL
  AND p95_range IS NOT NULL
  AND alt_dir_share >= 0.60
  AND median_body_frac <= 0.10
  AND SAFE_DIVIDE(p95_range, NULLIF(p05_range,0)) <= 5
  AND close_vol_ratio BETWEEN 0.01 AND 0.20