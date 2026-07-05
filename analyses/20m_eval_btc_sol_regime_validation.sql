-- Validation (§6) for the BTC/SOL OHLCV market-regime features added to
-- rl_inference_features_next_open_v (build spec 2026-07-05).
-- Convention: every row reports failing_rows; 0 == pass.
--
-- Note on warm-up: these are single-series features broadcast by price_timestamp;
-- the BTC/SOL candle history predates the altcoin view window, so they are
-- generally fully warm at the view's first timestamp. Warm-up NULL gating is
-- validated in the per-token suite (20m_eval_ohlc_feature_validation) against the
-- same rl_ohlc_candle_features table these are sourced from.

WITH b AS (
  SELECT * FROM {{ ref('rl_inference_features_next_open_v') }}
),

-- §7.4 broadcast: one distinct value per timestamp for each btc_/sol_ column.
broadcast AS (
  SELECT
    price_timestamp,
    COUNT(DISTINCT btc_flow_imbalance_24h) AS nd_btc_flow,
    COUNT(DISTINCT btc_cs_spread_24h_bps)  AS nd_btc_cs,
    COUNT(DISTINCT btc_adx_24h)            AS nd_btc_adx,
    COUNT(DISTINCT btc_atr_24h)            AS nd_btc_atr,
    COUNT(DISTINCT btc_dist_to_true_high_24h) AS nd_btc_disthigh,
    COUNT(DISTINCT sol_flow_imbalance_24h) AS nd_sol_flow,
    COUNT(DISTINCT sol_cs_spread_24h_bps)  AS nd_sol_cs,
    COUNT(DISTINCT sol_adx_24h)            AS nd_sol_adx,
    COUNT(DISTINCT sol_atr_24h)            AS nd_sol_atr,
    COUNT(DISTINCT sol_dist_to_true_high_24h) AS nd_sol_disthigh
  FROM b
  GROUP BY price_timestamp
)

-- ===================== §7.4 join-broadcast (critical) =====================
SELECT 'broadcast_btc_single_value' AS check_name,
  COUNTIF(nd_btc_flow > 1 OR nd_btc_cs > 1 OR nd_btc_adx > 1
       OR nd_btc_atr > 1 OR nd_btc_disthigh > 1) AS failing_rows,
  'every altcoin row at a timestamp must carry one btc_* value (COUNT DISTINCT <= 1)' AS detail
FROM broadcast

UNION ALL
SELECT 'broadcast_sol_single_value',
  COUNTIF(nd_sol_flow > 1 OR nd_sol_cs > 1 OR nd_sol_adx > 1
       OR nd_sol_atr > 1 OR nd_sol_disthigh > 1),
  'every altcoin row at a timestamp must carry one sol_* value (COUNT DISTINCT <= 1)'
FROM broadcast

-- ===================== §6.3 bounds (zero tolerance) =====================
UNION ALL
SELECT 'bounds_btc_flow_imbalance',
  COUNTIF(btc_flow_imbalance_24h  NOT BETWEEN -1 AND 1
       OR btc_flow_imbalance_168h NOT BETWEEN -1 AND 1),
  'btc_flow_imbalance_* must be in [-1, 1]'
FROM b

UNION ALL
SELECT 'bounds_btc_clv_mean',
  COUNTIF(btc_clv_mean_24h NOT BETWEEN -1 AND 1),
  'btc_clv_mean_24h must be in [-1, 1]'
FROM b

UNION ALL
SELECT 'bounds_btc_mfi',
  COUNTIF(btc_mfi_24h NOT BETWEEN 0 AND 100),
  'btc_mfi_24h must be in [0, 100]'
FROM b

UNION ALL
SELECT 'bounds_btc_rank_0_1',
  COUNTIF(btc_nr_pctrank_24h        NOT BETWEEN 0 AND 1
       OR btc_squeeze_pctile_720h    NOT BETWEEN 0 AND 1
       OR btc_true_range_pos_168h    NOT BETWEEN 0 AND 1
       OR btc_bars_since_true_high_168h NOT BETWEEN 0 AND 1
       OR btc_er_24h NOT BETWEEN 0 AND 1
       OR btc_er_168h NOT BETWEEN 0 AND 1),
  'btc pctrank / pctile / range-pos / bars-since / er features must be in [0, 1]'
FROM b

UNION ALL
SELECT 'bounds_btc_dist_true',
  COUNTIF(btc_dist_to_true_high_24h  > 0
       OR btc_dist_to_true_high_168h > 0
       OR btc_dist_to_true_high_720h > 0
       OR btc_dist_to_true_low_24h  < 0
       OR btc_dist_to_true_low_168h < 0),
  'btc_dist_to_true_high_* <= 0 and btc_dist_to_true_low_* >= 0'
FROM b

UNION ALL
SELECT 'bounds_btc_cs_spread_ge0',
  COUNTIF(btc_cs_spread_24h_bps < 0),
  'btc_cs_spread_24h_bps must be >= 0'
FROM b

UNION ALL
SELECT 'bounds_btc_choppiness',
  COUNTIF(btc_choppiness_168h NOT BETWEEN 0 AND 100),
  'btc_choppiness_168h must be in [0, 100]'
FROM b

UNION ALL
SELECT 'bounds_btc_breakout_binary',
  COUNTIF(btc_true_breakout_high_24h NOT IN (0, 1)
       OR btc_true_breakout_low_24h  NOT IN (0, 1)),
  'btc_true_breakout_* must be 0 or 1'
FROM b

UNION ALL
SELECT 'bounds_sol_flow_imbalance',
  COUNTIF(sol_flow_imbalance_24h  NOT BETWEEN -1 AND 1
       OR sol_flow_imbalance_168h NOT BETWEEN -1 AND 1),
  'sol_flow_imbalance_* must be in [-1, 1]'
FROM b

UNION ALL
SELECT 'bounds_sol_clv_mean',
  COUNTIF(sol_clv_mean_24h NOT BETWEEN -1 AND 1),
  'sol_clv_mean_24h must be in [-1, 1]'
FROM b

UNION ALL
SELECT 'bounds_sol_mfi',
  COUNTIF(sol_mfi_24h NOT BETWEEN 0 AND 100),
  'sol_mfi_24h must be in [0, 100]'
FROM b

UNION ALL
SELECT 'bounds_sol_rank_0_1',
  COUNTIF(sol_nr_pctrank_24h        NOT BETWEEN 0 AND 1
       OR sol_squeeze_pctile_720h    NOT BETWEEN 0 AND 1
       OR sol_true_range_pos_168h    NOT BETWEEN 0 AND 1
       OR sol_bars_since_true_high_168h NOT BETWEEN 0 AND 1
       OR sol_er_24h NOT BETWEEN 0 AND 1
       OR sol_er_168h NOT BETWEEN 0 AND 1),
  'sol pctrank / pctile / range-pos / bars-since / er features must be in [0, 1]'
FROM b

UNION ALL
SELECT 'bounds_sol_dist_true',
  COUNTIF(sol_dist_to_true_high_24h  > 0
       OR sol_dist_to_true_high_168h > 0
       OR sol_dist_to_true_high_720h > 0
       OR sol_dist_to_true_low_24h  < 0
       OR sol_dist_to_true_low_168h < 0),
  'sol_dist_to_true_high_* <= 0 and sol_dist_to_true_low_* >= 0'
FROM b

UNION ALL
SELECT 'bounds_sol_cs_spread_ge0',
  COUNTIF(sol_cs_spread_24h_bps < 0),
  'sol_cs_spread_24h_bps must be >= 0'
FROM b

UNION ALL
SELECT 'bounds_sol_choppiness',
  COUNTIF(sol_choppiness_168h NOT BETWEEN 0 AND 100),
  'sol_choppiness_168h must be in [0, 100]'
FROM b

UNION ALL
SELECT 'bounds_sol_breakout_binary',
  COUNTIF(sol_true_breakout_high_24h NOT IN (0, 1)
       OR sol_true_breakout_low_24h  NOT IN (0, 1)),
  'sol_true_breakout_* must be 0 or 1'
FROM b
