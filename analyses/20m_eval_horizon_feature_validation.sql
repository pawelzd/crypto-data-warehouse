-- Validation (§5) for the horizon/rotation/regime features added to
-- rl_inference_features_next_open_v (build spec 2026-07-07).
-- Convention: every row reports failing_rows; 0 == pass. Rank-center rows are
-- statistical (large-sample).

WITH b AS (
  SELECT
    *,
    ROW_NUMBER() OVER (PARTITION BY token_address ORDER BY price_timestamp) AS token_rn
  FROM {{ ref('rl_inference_features_next_open_v') }}
),

broadcast AS (
  SELECT
    price_timestamp,
    COUNT(DISTINCT sol_dist_to_sma_50d)  AS nd_sol_sma,
    COUNT(DISTINCT sol_cumret_30d)       AS nd_sol_cumret,
    COUNT(DISTINCT sol_trend_slope_90d)  AS nd_sol_slope,
    COUNT(DISTINCT univ_pct_above_sma_720h) AS nd_univ_sma,
    COUNT(DISTINCT univ_med_cumret_30d)  AS nd_univ_cumret
  FROM b
  GROUP BY price_timestamp
),

rank_center AS (
  SELECT
    AVG(rel_rank_cumret_14d)   AS avg_rank_14d,
    AVG(rel_rank_cumret_30d)   AS avg_rank_30d,
    AVG(rel_rank_drawdown_7d)  AS avg_rank_dd
  FROM b
  WHERE active_universe AND rel_rank_cumret_14d IS NOT NULL
)

-- ===================== §5.3 bounds (zero tolerance) =====================
SELECT 'bounds_log_gap_le0' AS check_name,
  COUNTIF(log_gap_from_ath > 0 OR log_gap_from_90d_high > 0) AS failing_rows,
  'log_gap_from_ath and log_gap_from_90d_high must be <= 0' AS detail
FROM b

UNION ALL
SELECT 'bounds_sol_drawdown_le0',
  COUNTIF(sol_drawdown_from_180d_high > 0),
  'sol_drawdown_from_180d_high must be <= 0'
FROM b

UNION ALL
SELECT 'bounds_drawdown_14d_30d',
  COUNTIF(drawdown_14d > 0 OR drawdown_14d < -1 OR drawdown_30d > 0 OR drawdown_30d < -1),
  'drawdown_14d/30d must be in [-1, 0] (price/MAX - 1 convention)'
FROM b

UNION ALL
SELECT 'bounds_ath_recency_frac_0_1',
  COUNTIF(ath_recency_frac NOT BETWEEN 0 AND 1),
  'ath_recency_frac must be in [0, 1]'
FROM b

UNION ALL
SELECT 'bounds_univ_breadth_0_1',
  COUNTIF(univ_pct_above_sma_720h NOT BETWEEN 0 AND 1
       OR univ_frac_near_30d_high NOT BETWEEN 0 AND 1),
  'univ_pct_above_sma_720h and univ_frac_near_30d_high must be in [0, 1]'
FROM b

UNION ALL
SELECT 'bounds_rel_rank_0_1',
  COUNTIF(rel_rank_cumret_14d  NOT BETWEEN 0 AND 1
       OR rel_rank_cumret_30d  NOT BETWEEN 0 AND 1
       OR rel_rank_drawdown_7d NOT BETWEEN 0 AND 1),
  'rel_rank_* must be in [0, 1]'
FROM b

-- ===================== §5.4 nesting invariants =====================
UNION ALL
SELECT 'nesting_log_gap',
  COUNTIF(log_gap_from_ath IS NOT NULL AND log_gap_from_90d_high IS NOT NULL
      AND log_gap_from_30d_high IS NOT NULL
      AND NOT (log_gap_from_ath <= log_gap_from_90d_high + 1e-9
           AND log_gap_from_90d_high <= log_gap_from_30d_high + 1e-9)),
  'log_gap_from_ath <= log_gap_from_90d_high <= log_gap_from_30d_high'
FROM b

UNION ALL
SELECT 'nesting_drawdown',
  COUNTIF(drawdown_30d IS NOT NULL AND drawdown_14d IS NOT NULL AND drawdown_7d IS NOT NULL
      AND NOT (drawdown_30d <= drawdown_14d + 1e-9
           AND drawdown_14d <= drawdown_7d + 1e-9)),
  'drawdown_30d <= drawdown_14d <= drawdown_7d (longer window is deeper)'
FROM b

-- ===================== §5.1 warm-up gating =====================
UNION ALL
SELECT 'warmup_cumret_30d',
  COUNTIF(token_rn < 720 AND cumret_30d IS NOT NULL),
  'cumret_30d must be NULL before 720 token rows'
FROM b

UNION ALL
SELECT 'warmup_log_gap_from_ath_bar1',
  COUNTIF(token_rn = 1 AND log_gap_from_ath IS NULL),
  'log_gap_from_ath is defined from bar 1 (unbounded window)'
FROM b

UNION ALL
SELECT 'warmup_ath_recency_frac_bar1',
  COUNTIF(token_rn = 1 AND ath_recency_frac IS NOT NULL),
  'ath_recency_frac is NULL on the first bar'
FROM b

-- ===================== §7.4-style broadcast (SOL + univ) =====================
UNION ALL
SELECT 'broadcast_sol_single_value',
  COUNTIF(nd_sol_sma > 1 OR nd_sol_cumret > 1 OR nd_sol_slope > 1),
  'each sol_* regime column has one distinct value per timestamp'
FROM broadcast

UNION ALL
SELECT 'broadcast_univ_single_value',
  COUNTIF(nd_univ_sma > 1 OR nd_univ_cumret > 1),
  'each univ_* breadth column has one distinct value per timestamp'
FROM broadcast

-- ===================== §5.3 rank centering (statistical) =====================
UNION ALL
SELECT 'rel_rank_center',
  IF(ABS(avg_rank_14d - 0.5) <= 0.02
 AND ABS(avg_rank_30d - 0.5) <= 0.02
 AND ABS(avg_rank_dd  - 0.5) <= 0.02, 0, 1),
  CONCAT('avg rel_ranks (14d/30d/dd) = ',
         CAST(avg_rank_14d AS STRING), ' / ',
         CAST(avg_rank_30d AS STRING), ' / ',
         CAST(avg_rank_dd  AS STRING), '; expected ~0.5')
FROM rank_center
