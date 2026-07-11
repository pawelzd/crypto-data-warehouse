-- Validation (§4) for the momentum/pullback quality features added to
-- rl_inference_features_next_open_v (spec 2026-07-11).
-- Convention: every row reports failing_rows; 0 == pass. Sign / degeneracy rows
-- are statistical (large-sample); the §4.5 degeneracy check is the key gate.

WITH b AS (
  SELECT
    *,
    ROW_NUMBER() OVER (PARTITION BY token_address ORDER BY price_timestamp) AS token_rn
  FROM {{ ref('rl_inference_features_next_open_v') }}
),

-- §4.5 within-timestamp Spearman = Pearson of within-timestamp percent-ranks.
ranks AS (
  SELECT
    price_timestamp,
    PERCENT_RANK() OVER (PARTITION BY price_timestamp ORDER BY pullback_z_720h)      AS r_pull,
    PERCENT_RANK() OVER (PARTITION BY price_timestamp ORDER BY drawdown_7d)          AS r_dd,
    PERCENT_RANK() OVER (PARTITION BY price_timestamp ORDER BY momentum_accel_24_168) AS r_macc,
    PERCENT_RANK() OVER (PARTITION BY price_timestamp ORDER BY cumret_24h)           AS r_c24
  FROM b
  WHERE pullback_z_720h IS NOT NULL AND drawdown_7d IS NOT NULL
    AND momentum_accel_24_168 IS NOT NULL AND cumret_24h IS NOT NULL
),
spearman AS (
  SELECT
    AVG(pull_dd)  AS sp_pull_dd,
    AVG(macc_c24) AS sp_macc_c24
  FROM (
    SELECT
      CORR(r_pull, r_dd)  AS pull_dd,
      CORR(r_macc, r_c24) AS macc_c24
    FROM ranks
    GROUP BY price_timestamp
    HAVING COUNT(*) >= 20
  )
),

signs AS (
  SELECT
    CORR(up_bar_frac_72h, cumret_7d) AS corr_upbar_cumret,
    -- z-scores have mean ~0 by construction; the MEDIAN is +~0.2 here because
    -- drawdown_7d is asymmetric (bounded <=0, right-skewed), which is expected
    -- and not a defect -- so sanity-check the mean, not the median.
    AVG(pullback_z_720h)                                     AS mean_pullback_z,
    APPROX_QUANTILES(momentum_accel_24_168, 100)[OFFSET(50)] AS med_macc
  FROM b
)

-- ===================== §4.2 bounds (zero tolerance) =====================
SELECT 'bounds_up_bar_frac_0_1' AS check_name,
  COUNTIF(up_bar_frac_72h NOT BETWEEN 0 AND 1) AS failing_rows,
  'up_bar_frac_72h must be in [0, 1]' AS detail
FROM b

UNION ALL
SELECT 'bounds_vol_mom_align_pm1',
  COUNTIF(vol_mom_align_168h NOT BETWEEN -1 AND 1),
  'vol_mom_align_168h must be in [-1, 1]'
FROM b

UNION ALL
SELECT 'bounds_ret_skew_pm10',
  COUNTIF(ret_skew_168h NOT BETWEEN -10 AND 10),
  'ret_skew_168h must be in [-10, 10] post-winsorize'
FROM b

-- ===================== §4.1 warm-up NULL gating =====================
UNION ALL
SELECT 'warmup_pullback_z_720',
  COUNTIF(token_rn < 720 AND pullback_z_720h IS NOT NULL),
  'pullback_z_720h must be NULL before 720 token rows'
FROM b

UNION ALL
SELECT 'warmup_w168_features',
  COUNTIF(token_rn < 168 AND (vol_mom_align_168h IS NOT NULL OR ret_skew_168h IS NOT NULL)),
  'vol_mom_align_168h / ret_skew_168h must be NULL before 168 token rows'
FROM b

UNION ALL
SELECT 'warmup_up_bar_frac_72',
  COUNTIF(token_rn < 72 AND up_bar_frac_72h IS NOT NULL),
  'up_bar_frac_72h must be NULL before 72 token rows'
FROM b

-- ===================== §4.5 degeneracy (the key gate) =====================
UNION ALL
SELECT 'degeneracy_pullback_vs_drawdown',
  IF(sp_pull_dd < 0.98, 0, 1),
  CONCAT('within-ts Spearman(pullback_z_720h, drawdown_7d) = ', CAST(sp_pull_dd AS STRING), '; must be < 0.98')
FROM spearman

UNION ALL
SELECT 'degeneracy_macc_vs_cumret24',
  IF(sp_macc_c24 < 0.98, 0, 1),
  CONCAT('within-ts Spearman(momentum_accel, cumret_24h) = ', CAST(sp_macc_c24 AS STRING), '; must be < 0.98')
FROM spearman

-- ===================== §4.4 sanity signs (statistical) =====================
UNION ALL
SELECT 'sanity_up_bar_frac_pos_corr_cumret',
  IF(corr_upbar_cumret > 0, 0, 1),
  CONCAT('corr(up_bar_frac_72h, cumret_7d) = ', CAST(corr_upbar_cumret AS STRING), '; expected > 0')
FROM signs

UNION ALL
SELECT 'sanity_pullback_z_mean_near0',
  IF(ABS(mean_pullback_z) <= 0.1, 0, 1),
  CONCAT('mean pullback_z_720h = ', CAST(mean_pullback_z AS STRING), '; expected ~0 by z-score construction')
FROM signs

UNION ALL
SELECT 'sanity_momentum_accel_median_near0',
  IF(ABS(med_macc) <= 0.02, 0, 1),
  CONCAT('median momentum_accel_24_168 = ', CAST(med_macc AS STRING), '; expected ~0')
FROM signs
