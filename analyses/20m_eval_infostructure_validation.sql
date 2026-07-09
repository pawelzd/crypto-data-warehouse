-- Validation (§7) for the information-structure features added to
-- rl_inference_features_next_open_v (novel-features spec 2026-07-08).
-- Convention: every row reports failing_rows; 0 == pass. Regime rows are
-- comparative (bull vs crash) and read as a sign/index sanity, not a threshold.

WITH b AS (
  SELECT
    *,
    ROW_NUMBER() OVER (PARTITION BY token_address ORDER BY price_timestamp) AS token_rn
  FROM {{ ref('rl_inference_features_next_open_v') }}
),

broadcast AS (
  SELECT
    price_timestamp,
    COUNT(DISTINCT idx_logret_1h)     AS nd_idx_logret,
    COUNT(DISTINCT idx_cumret_30d)    AS nd_idx_cumret,
    COUNT(DISTINCT herding_ratio_168h) AS nd_herding
  FROM b
  GROUP BY price_timestamp
),

regime AS (
  SELECT
    AVG(IF(DATE(price_timestamp) BETWEEN '2026-04-01' AND '2026-05-14', herding_ratio_168h, NULL)) AS herding_bull,
    AVG(IF(DATE(price_timestamp) BETWEEN '2026-02-01' AND '2026-02-28', herding_ratio_168h, NULL)) AS herding_crash,
    AVG(IF(DATE(price_timestamp) BETWEEN '2026-04-01' AND '2026-05-14', downside_semivar_share_24h, NULL)) AS dsv_bull,
    AVG(IF(DATE(price_timestamp) BETWEEN '2026-02-01' AND '2026-02-28', downside_semivar_share_24h, NULL)) AS dsv_crash,
    AVG(IF(DATE(price_timestamp) BETWEEN '2026-04-01' AND '2026-05-14', vpin_z_30d, NULL)) AS vpinz_bull,
    AVG(IF(DATE(price_timestamp) BETWEEN '2026-02-01' AND '2026-02-28', vpin_z_30d, NULL)) AS vpinz_crash
  FROM b
)

-- ===================== §7.2 bounds (zero tolerance) =====================
SELECT 'bounds_share_0_1' AS check_name,
  COUNTIF(idio_vol_share_60d NOT BETWEEN 0 AND 1
       OR vpin_24h  NOT BETWEEN 0 AND 1
       OR vpin_168h NOT BETWEEN 0 AND 1
       OR jump_share_24h  NOT BETWEEN 0 AND 1
       OR jump_share_168h NOT BETWEEN 0 AND 1
       OR downside_semivar_share_24h NOT BETWEEN 0 AND 1) AS failing_rows,
  'idio_vol_share / vpin_* / jump_share_* / downside_semivar_share must be in [0,1]' AS detail
FROM b

UNION ALL
SELECT 'bounds_beta_clamp',
  COUNTIF(beta_sol_60d NOT BETWEEN -2 AND 5 OR beta_idx_60d NOT BETWEEN -2 AND 5),
  'beta_* must be in [-2, 5] post-winsorize'
FROM b

UNION ALL
SELECT 'bounds_dormancy_reawaken_ge0',
  COUNTIF(dormancy_days_log < 0 OR reawakening_score < 0),
  'dormancy_days_log and reawakening_score must be >= 0'
FROM b

UNION ALL
SELECT 'bounds_herding_gt0',
  COUNTIF(herding_ratio_168h <= 0 OR herding_ratio_720h <= 0),
  'herding_ratio_* must be > 0'
FROM b

-- ===================== §7.3 identity (beta residual) =====================
UNION ALL
SELECT 'identity_idio_mom_sol',
  COUNTIF(beta_sol_60d IS NOT NULL AND sol_cumret_7d IS NOT NULL
      AND ABS(idio_mom_sol_7d + beta_sol_60d * sol_cumret_7d - cumret_7d) > 1e-6),
  'idio_mom_sol_7d + beta_sol_60d * sol_cumret_7d == cumret_7d'
FROM b

-- ===================== §7.1 warm-up gating =====================
UNION ALL
SELECT 'warmup_beta_1440',
  COUNTIF(token_rn < 1440 AND beta_sol_60d IS NOT NULL),
  'beta_sol_60d (w1440) must be NULL before 1440 token rows'
FROM b

UNION ALL
SELECT 'warmup_vpin_24h',
  COUNTIF(token_rn < 24 AND vpin_24h IS NOT NULL),
  'vpin_24h must be NULL before 24 token rows'
FROM b

-- ===================== broadcast (idx_* / herding_*) =====================
UNION ALL
SELECT 'broadcast_idx_herding_single_value',
  COUNTIF(nd_idx_logret > 1 OR nd_idx_cumret > 1 OR nd_herding > 1),
  'idx_* and herding_* have one distinct value per timestamp'
FROM broadcast

-- ===================== §7.4 regime sanity (comparative) =====================
UNION ALL
SELECT 'regime_herding_crash_gt_bull',
  IF(herding_crash > herding_bull, 0, 1),
  CONCAT('herding_ratio_168h crash=', CAST(herding_crash AS STRING), ' vs bull=', CAST(herding_bull AS STRING), ' (expect crash > bull)')
FROM regime

UNION ALL
SELECT 'regime_downside_semivar_crash_gt_bull',
  IF(dsv_crash > dsv_bull, 0, 1),
  CONCAT('downside_semivar_share_24h crash=', CAST(dsv_crash AS STRING), ' vs bull=', CAST(dsv_bull AS STRING), ' (expect crash > bull)')
FROM regime

UNION ALL
SELECT 'regime_vpin_z_crash_gt_bull',
  IF(vpinz_crash > vpinz_bull, 0, 1),
  CONCAT('vpin_z_30d crash=', CAST(vpinz_crash AS STRING), ' vs bull=', CAST(vpinz_bull AS STRING), ' (expect crash > bull)')
FROM regime
