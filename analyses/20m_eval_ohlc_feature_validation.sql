-- Validation suite (§8) for the OHLC candle features added to
-- rl_inference_features_next_open_v (build spec 2026-07-03).
-- Convention: every row reports failing_rows; 0 == pass. Estimator/breadth
-- "sanity" checks are statistical and should be read over a large sample.

WITH b AS (
  SELECT
    *,
    ROW_NUMBER() OVER (PARTITION BY token_address ORDER BY price_timestamp) AS token_rn
  FROM {{ ref('rl_inference_features_next_open_v') }}
),

-- §8.6 universe basis: OHLC univ aggregates must be built from active rows.
univ_counts AS (
  SELECT
    price_timestamp,
    COUNTIF(active_universe) AS active_count,
    COUNTIF(active_universe AND flow_imbalance_168h IS NOT NULL) AS active_with_flow
  FROM b
  GROUP BY price_timestamp
),

rel_center AS (
  SELECT AVG(rel_rank_flow_imbalance_168h) AS avg_rel_flow
  FROM b
  WHERE active_universe AND rel_rank_flow_imbalance_168h IS NOT NULL
),

estimator AS (
  SELECT
    APPROX_QUANTILES(rv_eff_ratio_24h, 100)[OFFSET(50)] AS med_rv_eff_ratio_24h,
    APPROX_QUANTILES(cs_spread_24h_bps, 100)[OFFSET(50)] AS med_cs_spread_24h_bps
  FROM b
  WHERE rv_eff_ratio_24h IS NOT NULL
)

-- ===================== §8.3 bounds (zero tolerance) =====================
SELECT 'bounds_clv_range' AS check_name,
  COUNTIF(clv_1h NOT BETWEEN -1 AND 1
       OR clv_mean_24h  NOT BETWEEN -1 AND 1
       OR clv_mean_168h NOT BETWEEN -1 AND 1) AS failing_rows,
  'clv_1h and clv_mean_* must be in [-1, 1]' AS detail
FROM b

UNION ALL
SELECT 'bounds_flow_imbalance',
  COUNTIF(flow_imbalance_24h  NOT BETWEEN -1 AND 1
       OR flow_imbalance_168h NOT BETWEEN -1 AND 1),
  'flow_imbalance_* must be in [-1, 1]'
FROM b

UNION ALL
SELECT 'bounds_mfi',
  COUNTIF(mfi_24h NOT BETWEEN 0 AND 100),
  'mfi_24h must be in [0, 100]'
FROM b

UNION ALL
SELECT 'bounds_frac_0_1',
  COUNTIF(uw_frac_mean_24h  NOT BETWEEN 0 AND 1
       OR lw_frac_mean_24h  NOT BETWEEN 0 AND 1
       OR max_uw_frac_24h   NOT BETWEEN 0 AND 1
       OR body_frac_mean_24h NOT BETWEEN 0 AND 1
       OR zero_range_frac_24h NOT BETWEEN 0 AND 1
       OR true_range_pos_168h NOT BETWEEN 0 AND 1),
  'fraction features must be in [0, 1]'
FROM b

UNION ALL
SELECT 'bounds_rank_pctile_0_1',
  COUNTIF(nr_pctrank_24h        NOT BETWEEN 0 AND 1
       OR squeeze_pctile_720h    NOT BETWEEN 0 AND 1
       OR btc_squeeze_pctile_720h NOT BETWEEN 0 AND 1
       OR sol_squeeze_pctile_720h NOT BETWEEN 0 AND 1
       OR rel_rank_flow_imbalance_168h NOT BETWEEN 0 AND 1
       OR rel_rank_atr_ratio_24_168    NOT BETWEEN 0 AND 1
       OR bars_since_true_high_168h    NOT BETWEEN 0 AND 1),
  'percent-rank / pctile / normalized-bars features must be in [0, 1]'
FROM b

UNION ALL
SELECT 'bounds_er_0_1',
  COUNTIF(er_24h NOT BETWEEN 0 AND 1 OR er_168h NOT BETWEEN 0 AND 1),
  'er_* (Kaufman efficiency) must be in [0, 1]'
FROM b

UNION ALL
SELECT 'bounds_dist_true_high_le0',
  COUNTIF(dist_to_true_high_24h  > 0
       OR dist_to_true_high_168h > 0
       OR dist_to_true_high_720h > 0),
  'dist_to_true_high_* must be <= 0 (window includes current bar)'
FROM b

UNION ALL
SELECT 'bounds_dist_true_low_ge0',
  COUNTIF(dist_to_true_low_24h < 0 OR dist_to_true_low_168h < 0),
  'dist_to_true_low_* must be >= 0'
FROM b

UNION ALL
SELECT 'bounds_cs_spread_ge0',
  COUNTIF(cs_spread_24h_bps < 0),
  'cs_spread_24h_bps must be >= 0'
FROM b

UNION ALL
SELECT 'bounds_choppiness_0_100',
  COUNTIF(choppiness_168h NOT BETWEEN 0 AND 100),
  'choppiness_168h must be in [0, 100]'
FROM b

UNION ALL
SELECT 'bounds_consec_inside_0_6',
  COUNTIF(consec_inside_6h NOT BETWEEN 0 AND 6),
  'consec_inside_6h must be in [0, 6]'
FROM b

UNION ALL
SELECT 'bounds_true_breakout_binary',
  COUNTIF(true_breakout_high_24h NOT IN (0, 1)
       OR true_breakout_low_24h  NOT IN (0, 1)),
  'true_breakout_* must be 0 or 1'
FROM b

UNION ALL
SELECT 'bounds_univ_frac_0_1',
  COUNTIF(univ_frac_true_breakout_high_24h NOT BETWEEN 0 AND 1
       OR univ_frac_true_breakout_low_24h  NOT BETWEEN 0 AND 1
       OR univ_frac_squeeze                NOT BETWEEN 0 AND 1),
  'univ_frac_* aggregates must be in [0, 1]'
FROM b

-- ===================== §8.1 warm-up NULL gating =====================
UNION ALL
SELECT 'warmup_clv_mean_24h',
  COUNTIF(token_rn < 24 AND clv_mean_24h IS NOT NULL),
  'clv_mean_24h must be NULL before 24 token rows'
FROM b

UNION ALL
SELECT 'warmup_atr_24h_lag',
  COUNTIF(token_rn < 25 AND atr_24h IS NOT NULL),
  'atr_24h (lag-dependent) must be NULL before 25 token rows'
FROM b

UNION ALL
SELECT 'warmup_parkinson_24h',
  COUNTIF(token_rn < 24 AND parkinson_rv_24h IS NOT NULL),
  'parkinson_rv_24h must be NULL before 24 token rows'
FROM b

UNION ALL
SELECT 'warmup_adx_24h',
  COUNTIF(token_rn < 48 AND adx_24h IS NOT NULL),
  'adx_24h (double-smoothed) must be NULL before 48 token rows'
FROM b

UNION ALL
SELECT 'warmup_ad_slope_168h',
  COUNTIF(token_rn < 168 AND ad_slope_168h IS NOT NULL),
  'ad_slope_168h must be NULL before 168 token rows'
FROM b

UNION ALL
SELECT 'warmup_squeeze_pctile_720h',
  COUNTIF(token_rn < 720 AND squeeze_pctile_720h IS NOT NULL),
  'squeeze_pctile_720h must be NULL before 720 token rows'
FROM b

UNION ALL
SELECT 'warmup_ad_price_diverge_168h',
  COUNTIF(token_rn < 720 AND ad_price_diverge_168h IS NOT NULL),
  'ad_price_diverge_168h must be NULL before 720 token rows'
FROM b

-- ===================== §8.6 universe consistency =====================
UNION ALL
SELECT 'universe_flow_basis',
  COUNTIF(active_count > 0 AND active_with_flow = 0),
  'timestamps with active tokens should have at least one non-null flow_imbalance_168h'
FROM univ_counts

UNION ALL
SELECT 'rel_flow_rank_center',
  IF(ABS(avg_rel_flow - 0.5) <= 0.02, 0, 1),
  CONCAT('avg rel_rank_flow_imbalance_168h = ', CAST(avg_rel_flow AS STRING), '; expected ~0.5')
FROM rel_center

-- ===================== §8.5 estimator sanity (statistical) =====================
UNION ALL
SELECT 'sanity_rv_eff_ratio_median',
  IF(med_rv_eff_ratio_24h BETWEEN 0.6 AND 2.0, 0, 1),
  CONCAT('median rv_eff_ratio_24h = ', CAST(med_rv_eff_ratio_24h AS STRING), '; expected ~[0.6, 2.0]')
FROM estimator

UNION ALL
SELECT 'sanity_cs_spread_median_positive',
  IF(med_cs_spread_24h_bps > 0, 0, 1),
  CONCAT('median cs_spread_24h_bps = ', CAST(med_cs_spread_24h_bps AS STRING), '; expected > 0')
FROM estimator
