{{ config(
    materialized = 'table'
) }}

-- ============================================================================
-- Per-token OHLC candle features (build spec 2026-07-03).
-- Extracted from rl_inference_features_next_open_v and materialized as a table
-- so the ~10 stacked window passes are planned/executed on their own; inlining
-- them into the main view exceeds BigQuery's query-planning complexity limit.
--
-- Source: token_ohlcv (the pipeline's only OHLC source; the existing price /
-- volume columns come from public_historical_prices, a different source).
-- dvol_1h uses the spec §1.1 dollar proxy volume * typ_price (token_ohlcv
-- volume unit unconfirmed). ad_price_diverge_168h uses a locally computed
-- OHLC log-close trend slope so both z-scores share one row series (noted
-- deviation from the spec, which named the existing trend_slope_7d).
-- Every rolling feature is causal (ROWS ... PRECEDING AND CURRENT ROW) and
-- full-window gated on row_idx (row_idx >= N; lag-dependent features >= N+1).
-- Keyed by (token_address, price_timestamp); the token universe is the eval
-- dataset tokens plus the btc/sol reference series.
-- ============================================================================
WITH ohlc_dedup AS (
  SELECT
    o.token_address,
    o.price_timestamp,
    SAFE_CAST(o.open   AS FLOAT64) AS open,
    SAFE_CAST(o.high   AS FLOAT64) AS high,
    SAFE_CAST(o.low    AS FLOAT64) AS low,
    SAFE_CAST(o.close  AS FLOAT64) AS close,
    SAFE_CAST(o.volume AS FLOAT64) AS volume
  FROM {{ ref('token_ohlcv') }} o
  WHERE o.token_address IN (SELECT DISTINCT token_address FROM {{ ref('20m_cv_prod_eval_dataset') }})
     OR o.token_address IN ('btcusdt', 'So11111111111111111111111111111111111111112')
  QUALIFY ROW_NUMBER() OVER (
    PARTITION BY o.token_address, o.price_timestamp
    ORDER BY SAFE_CAST(o.volume AS FLOAT64) DESC
  ) = 1
),

-- Per-bar scalars (§3.1). No cross-bar terms yet.
ohlc_scalars AS (
  SELECT
    d.token_address,
    d.price_timestamp,
    d.open, d.high, d.low, d.close, d.volume,
    (d.high + d.low + d.close) / 3.0                              AS typ_price_1h,
    d.volume * ((d.high + d.low + d.close) / 3.0)                 AS dvol_1h,
    d.high - d.low                                                AS range_raw_1h,
    SAFE_DIVIDE(d.high - d.low, d.close)                          AS range_1h,
    COALESCE(SAFE.LN(SAFE_DIVIDE(d.high, d.low)), 0.0)            AS log_hl_1h,
    COALESCE(SAFE_DIVIDE(2*d.close - d.high - d.low, d.high - d.low), 0.0) AS clv_1h,
    COALESCE(SAFE_DIVIDE(ABS(d.close - d.open), d.high - d.low), 0.0)      AS body_frac_1h,
    COALESCE(SAFE_DIVIDE(d.high - GREATEST(d.open, d.close), d.high - d.low), 0.0) AS uw_frac_1h,
    COALESCE(SAFE_DIVIDE(LEAST(d.open, d.close) - d.low, d.high - d.low), 0.0)     AS lw_frac_1h,
    COALESCE(SAFE.LN(SAFE_DIVIDE(d.close, d.open)),  0.0)         AS ln_co,
    SAFE.LN(d.high)                                               AS ln_high,
    SAFE.LN(d.low)                                                AS ln_low,
    SAFE.LN(d.close)                                              AS ln_close,
    COALESCE(SAFE.LN(SAFE_DIVIDE(d.high, d.close)), 0.0)         AS ln_hc,
    COALESCE(SAFE.LN(SAFE_DIVIDE(d.high, d.open)),  0.0)         AS ln_ho,
    COALESCE(SAFE.LN(SAFE_DIVIDE(d.low,  d.close)), 0.0)         AS ln_lc,
    COALESCE(SAFE.LN(SAFE_DIVIDE(d.low,  d.open)),  0.0)         AS ln_lo,
    ROW_NUMBER() OVER w                                           AS row_idx,
    LAG(d.close) OVER w                                           AS prev_close,
    LAG(d.high)  OVER w                                           AS prev_high,
    LAG(d.low)   OVER w                                           AS prev_low
  FROM ohlc_dedup d
  WINDOW w AS (PARTITION BY d.token_address ORDER BY d.price_timestamp)
),

-- Cross-bar per-bar terms (LAG-based helpers).
ohlc_bar AS (
  SELECT
    s.*,
    s.clv_1h * s.dvol_1h                                          AS signed_dvol_1h,
    s.typ_price_1h * s.dvol_1h                                    AS typ_dvol_1h,
    s.ln_close - LAG(s.ln_close) OVER w                           AS logret_close_1h,
    GREATEST(s.range_raw_1h, ABS(s.high - s.prev_close), ABS(s.low - s.prev_close)) AS tr_raw_1h,
    SAFE_DIVIDE(
      GREATEST(s.range_raw_1h, ABS(s.high - s.prev_close), ABS(s.low - s.prev_close)),
      s.prev_close
    )                                                             AS tr_1h,
    SAFE_DIVIDE(s.open, s.prev_close) - 1                         AS gap_1h,
    IF(s.high < s.prev_high AND s.low > s.prev_low, 1, 0)         AS inside_1h,
    ABS(s.close - s.prev_close)                                   AS abs_dclose_1h,
    LAG(s.close, 24)  OVER w                                      AS close_lag24_1h,
    LAG(s.close, 168) OVER w                                      AS close_lag168_1h,
    IF(s.typ_price_1h > LAG(s.typ_price_1h) OVER w, s.typ_price_1h * s.dvol_1h, 0.0) AS mf_pos_1h,
    IF(s.typ_price_1h < LAG(s.typ_price_1h) OVER w, s.typ_price_1h * s.dvol_1h, 0.0) AS mf_neg_1h,
    IF((s.high - s.prev_high) > (s.prev_low - s.low) AND (s.high - s.prev_high) > 0,
       s.high - s.prev_high, 0.0)                                 AS plus_dm_1h,
    IF((s.prev_low - s.low) > (s.high - s.prev_high) AND (s.prev_low - s.low) > 0,
       s.prev_low - s.low, 0.0)                                   AS minus_dm_1h,
    ABS(s.high - s.prev_low)                                      AS vm_plus_1h,
    ABS(s.low - s.prev_high)                                      AS vm_minus_1h,
    POW(s.log_hl_1h, 2) + POW(LAG(s.log_hl_1h) OVER w, 2)         AS cs_beta_1h,
    POW(COALESCE(SAFE.LN(SAFE_DIVIDE(GREATEST(s.high, s.prev_high), LEAST(s.low, s.prev_low))), 0.0), 2) AS cs_gamma_1h,
    (s.ln_high + s.ln_low) / 2.0                                  AS eta_t_1h,
    (LAG(s.ln_high) OVER w + LAG(s.ln_low) OVER w) / 2.0          AS eta_prev_1h,
    LAG(s.ln_close) OVER w                                        AS c_prev_1h,
    0.5 * POW(s.log_hl_1h, 2) - (2.0*LN(2) - 1.0) * POW(s.ln_co, 2) AS gk_term_1h,
    s.ln_hc * s.ln_ho + s.ln_lc * s.ln_lo                        AS rs_term_1h
  FROM ohlc_scalars s
  WINDOW w AS (PARTITION BY s.token_address ORDER BY s.price_timestamp)
),

-- Per-bar derived estimators (Corwin-Schultz s_t, Abdi-Ranaldo x_t, impact).
ohlc_derived AS (
  SELECT
    b.*,
    SAFE_DIVIDE(b.range_1h, NULLIF(b.dvol_1h, 0.0))              AS range_over_dvol_1h,
    (b.c_prev_1h - b.eta_prev_1h) * (b.c_prev_1h - b.eta_t_1h)   AS ar_x_1h,
    CASE
      WHEN b.cs_beta_1h IS NULL OR b.cs_gamma_1h IS NULL THEN NULL
      WHEN ((SQRT(2.0*b.cs_beta_1h) - SQRT(b.cs_beta_1h)) / (3.0 - 2.0*SQRT(2.0)))
            - SQRT(b.cs_gamma_1h / (3.0 - 2.0*SQRT(2.0))) <= 0 THEN 0.0
      ELSE
        2.0 * (EXP(
          ((SQRT(2.0*b.cs_beta_1h) - SQRT(b.cs_beta_1h)) / (3.0 - 2.0*SQRT(2.0)))
           - SQRT(b.cs_gamma_1h / (3.0 - 2.0*SQRT(2.0)))
        ) - 1.0)
        / (1.0 + EXP(
          ((SQRT(2.0*b.cs_beta_1h) - SQRT(b.cs_beta_1h)) / (3.0 - 2.0*SQRT(2.0)))
           - SQRT(b.cs_gamma_1h / (3.0 - 2.0*SQRT(2.0)))
        ))
    END                                                          AS cs_s_1h
  FROM ohlc_bar b
),

-- First window pass (w24 / w168 / w720 / w24_excl).
ohlc_win1 AS (
  SELECT
    o.token_address, o.price_timestamp, o.row_idx,
    o.close, o.high, o.low, o.range_1h, o.inside_1h, o.clv_1h,
    o.signed_dvol_1h, o.dvol_1h, o.logret_close_1h,
    -- §4A order flow / accumulation
    IF(o.row_idx >= 24,  AVG(o.clv_1h) OVER w24,  NULL)          AS clv_mean_24h,
    IF(o.row_idx >= 168, AVG(o.clv_1h) OVER w168, NULL)          AS clv_mean_168h,
    IF(o.row_idx >= 24,  SAFE_DIVIDE(SUM(o.signed_dvol_1h) OVER w24,  NULLIF(SUM(o.dvol_1h) OVER w24, 0.0)),  NULL) AS flow_imbalance_24h,
    IF(o.row_idx >= 168, SAFE_DIVIDE(SUM(o.signed_dvol_1h) OVER w168, NULLIF(SUM(o.dvol_1h) OVER w168, 0.0)), NULL) AS flow_imbalance_168h,
    IF(o.row_idx >= 25,  100.0 * SAFE_DIVIDE(SUM(o.mf_pos_1h) OVER w24, NULLIF(SUM(o.mf_pos_1h) OVER w24 + SUM(o.mf_neg_1h) OVER w24, 0.0)), NULL) AS mfi_24h,
    IF(o.row_idx >= 24,  SAFE.LN(SAFE_DIVIDE(o.close, SAFE_DIVIDE(SUM(o.typ_dvol_1h) OVER w24,  NULLIF(SUM(o.dvol_1h) OVER w24, 0.0)))),  NULL) AS vwap_dist_24h,
    IF(o.row_idx >= 168, SAFE.LN(SAFE_DIVIDE(o.close, SAFE_DIVIDE(SUM(o.typ_dvol_1h) OVER w168, NULLIF(SUM(o.dvol_1h) OVER w168, 0.0)))), NULL) AS vwap_dist_168h,
    IF(o.row_idx >= 24,  AVG(o.lw_frac_1h - o.uw_frac_1h) OVER w24, NULL) AS wick_asym_24h,
    -- §4B range volatility & compression
    IF(o.row_idx >= 25,  AVG(o.tr_1h) OVER w24,  NULL)          AS atr_24h,
    IF(o.row_idx >= 169, AVG(o.tr_1h) OVER w168, NULL)          AS atr_168h,
    IF(o.row_idx >= 24,  SQRT(SAFE_DIVIDE(AVG(POW(o.log_hl_1h, 2)) OVER w24,  4.0*LN(2))), NULL) AS parkinson_rv_24h,
    IF(o.row_idx >= 168, SQRT(SAFE_DIVIDE(AVG(POW(o.log_hl_1h, 2)) OVER w168, 4.0*LN(2))), NULL) AS parkinson_rv_7d,
    AVG(o.range_1h) OVER w24                                     AS _range_mean_24h,
    STDDEV_SAMP(o.range_1h) OVER w24                             AS _range_std_24h,
    IF(o.row_idx >= 24,  AVG(IF(o.high = o.low, 1.0, 0.0)) OVER w24, NULL) AS zero_range_frac_24h,
    -- §4C extremes / breakouts / path
    IF(o.row_idx >= 24,  COALESCE(SAFE.LN(SAFE_DIVIDE(o.close, MAX(o.high) OVER w24)),  0.0), NULL) AS dist_to_true_high_24h,
    IF(o.row_idx >= 168, COALESCE(SAFE.LN(SAFE_DIVIDE(o.close, MAX(o.high) OVER w168)), 0.0), NULL) AS dist_to_true_high_168h,
    IF(o.row_idx >= 720, COALESCE(SAFE.LN(SAFE_DIVIDE(o.close, MAX(o.high) OVER w720)), 0.0), NULL) AS dist_to_true_high_720h,
    IF(o.row_idx >= 24,  COALESCE(SAFE.LN(SAFE_DIVIDE(o.close, MIN(o.low) OVER w24)),   0.0), NULL) AS dist_to_true_low_24h,
    IF(o.row_idx >= 168, COALESCE(SAFE.LN(SAFE_DIVIDE(o.close, MIN(o.low) OVER w168)),  0.0), NULL) AS dist_to_true_low_168h,
    IF(o.row_idx >= 25,  IF(o.close > MAX(o.high) OVER w24x, 1.0, 0.0), NULL) AS true_breakout_high_24h,
    IF(o.row_idx >= 25,  IF(o.close < MIN(o.low)  OVER w24x, 1.0, 0.0), NULL) AS true_breakout_low_24h,
    IF(o.row_idx >= 168, SAFE_DIVIDE(o.close - MIN(o.low) OVER w168, NULLIF(MAX(o.high) OVER w168 - MIN(o.low) OVER w168, 0.0)), NULL) AS true_range_pos_168h,
    -- §4D microstructure
    IF(o.row_idx >= 25,  AVG(o.cs_s_1h) OVER w24 * 10000.0, NULL) AS cs_spread_24h_bps,
    IF(o.row_idx >= 24,  AVG(o.range_over_dvol_1h) OVER w24, NULL) AS range_impact_24h,
    -- adl normalizer & cumulative log price (used downstream)
    SUM(o.dvol_1h) OVER w24                                      AS dvol_24h,
    SUM(o.logret_close_1h) OVER wcum                             AS _logclose_rel,
    -- §7 tier-2 directional / efficiency intermediates
    100.0 * SAFE_DIVIDE(SUM(o.plus_dm_1h)  OVER w24,  NULLIF(SUM(o.tr_raw_1h) OVER w24, 0.0))  AS di_plus_24h,
    100.0 * SAFE_DIVIDE(SUM(o.minus_dm_1h) OVER w24,  NULLIF(SUM(o.tr_raw_1h) OVER w24, 0.0))  AS di_minus_24h,
    100.0 * SAFE_DIVIDE(SUM(o.plus_dm_1h)  OVER w168, NULLIF(SUM(o.tr_raw_1h) OVER w168, 0.0)) AS di_plus_168h,
    100.0 * SAFE_DIVIDE(SUM(o.minus_dm_1h) OVER w168, NULLIF(SUM(o.tr_raw_1h) OVER w168, 0.0)) AS di_minus_168h,
    IF(o.row_idx >= 169, 100.0 * SAFE_DIVIDE(
        SAFE.LOG(SAFE_DIVIDE(SUM(o.tr_raw_1h) OVER w168, NULLIF(MAX(o.high) OVER w168 - MIN(o.low) OVER w168, 0.0)), 10.0),
        LOG(168.0, 10.0)), NULL)                                 AS choppiness_168h,
    IF(o.row_idx >= 25,
       SAFE_DIVIDE(SUM(o.vm_plus_1h)  OVER w24, NULLIF(SUM(o.tr_raw_1h) OVER w24, 0.0))
     - SAFE_DIVIDE(SUM(o.vm_minus_1h) OVER w24, NULLIF(SUM(o.tr_raw_1h) OVER w24, 0.0)), NULL) AS vortex_24h,
    IF(o.row_idx >= 25,  SAFE_DIVIDE(ABS(o.close - o.close_lag24_1h),  NULLIF(SUM(o.abs_dclose_1h) OVER w24,  0.0)), NULL) AS er_24h,
    IF(o.row_idx >= 169, SAFE_DIVIDE(ABS(o.close - o.close_lag168_1h), NULLIF(SUM(o.abs_dclose_1h) OVER w168, 0.0)), NULL) AS er_168h,
    IF(o.row_idx >= 24,  SQRT(GREATEST(AVG(o.gk_term_1h) OVER w24, 0.0)), NULL) AS gk_rv_24h,
    IF(o.row_idx >= 24,  SQRT(GREATEST(AVG(o.rs_term_1h) OVER w24, 0.0)), NULL) AS rs_rv_24h,
    IF(o.row_idx >= 25,  AVG(ABS(o.gap_1h)) OVER w24, NULL)      AS gap_abs_mean_24h,
    IF(o.row_idx >= 169, MAX(ABS(o.gap_1h)) OVER w168, NULL)     AS max_gap_168h,
    IF(o.row_idx >= 24,  AVG(o.uw_frac_1h) OVER w24, NULL)       AS uw_frac_mean_24h,
    IF(o.row_idx >= 24,  AVG(o.lw_frac_1h) OVER w24, NULL)       AS lw_frac_mean_24h,
    IF(o.row_idx >= 24,  MAX(o.uw_frac_1h) OVER w24, NULL)       AS max_uw_frac_24h,
    IF(o.row_idx >= 24,  AVG(o.body_frac_1h) OVER w24, NULL)     AS body_frac_mean_24h,
    IF(o.row_idx >= 25,  2.0 * SQRT(GREATEST(AVG(o.ar_x_1h) OVER w24, 0.0)) * 10000.0, NULL) AS ar_spread_24h_bps
  FROM ohlc_derived o
  WINDOW
    w24  AS (PARTITION BY o.token_address ORDER BY o.price_timestamp ROWS BETWEEN 23  PRECEDING AND CURRENT ROW),
    w168 AS (PARTITION BY o.token_address ORDER BY o.price_timestamp ROWS BETWEEN 167 PRECEDING AND CURRENT ROW),
    w720 AS (PARTITION BY o.token_address ORDER BY o.price_timestamp ROWS BETWEEN 719 PRECEDING AND CURRENT ROW),
    w24x AS (PARTITION BY o.token_address ORDER BY o.price_timestamp ROWS BETWEEN 24  PRECEDING AND 1 PRECEDING),
    wcum AS (PARTITION BY o.token_address ORDER BY o.price_timestamp ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
),

-- Second window pass: z-scores, slopes, per-window arrays, adl component.
ohlc_win2 AS (
  SELECT
    w.*,
    IF(w.row_idx >= 24, SAFE_DIVIDE(w.range_1h - w._range_mean_24h, NULLIF(w._range_std_24h, 0.0)), NULL) AS range_z_24h,
    IF(w.row_idx >= 192, SAFE_DIVIDE(
        w.cs_spread_24h_bps - AVG(w.cs_spread_24h_bps) OVER w168,
        NULLIF(STDDEV_SAMP(w.cs_spread_24h_bps) OVER w168, 0.0)), NULL) AS cs_spread_z_168h,
    w.di_plus_24h  - w.di_minus_24h                             AS di_diff_24h,
    100.0 * SAFE_DIVIDE(ABS(w.di_plus_24h  - w.di_minus_24h),  NULLIF(w.di_plus_24h  + w.di_minus_24h,  0.0)) AS dx_24h,
    100.0 * SAFE_DIVIDE(ABS(w.di_plus_168h - w.di_minus_168h), NULLIF(w.di_plus_168h + w.di_minus_168h, 0.0)) AS dx_168h,
    SAFE_DIVIDE(w.signed_dvol_1h, NULLIF(w.dvol_24h, 0.0))      AS adl_component,
    IF(w.row_idx >= 168, SAFE_DIVIDE(
        COVAR_POP(w._logclose_rel, CAST(w.row_idx AS FLOAT64)) OVER w168,
        NULLIF(VAR_POP(CAST(w.row_idx AS FLOAT64)) OVER w168, 0.0)), NULL) AS _trend_slope_7d_ohlc,
    -- analytic ARRAY_AGG cannot use IGNORE NULLS in BigQuery; COALESCE warmup
    -- NULLs to a -1.0 sentinel (atr/range/high are all >= 0) and filter it out
    -- in the consuming subqueries below.
    ARRAY_AGG(COALESCE(w.atr_24h, -1.0)) OVER w720             AS _arr_atr_720,
    ARRAY_AGG(COALESCE(w.range_1h, -1.0)) OVER w24             AS _arr_range_24,
    ARRAY_AGG(COALESCE(w.high, -1.0)) OVER w168                AS _arr_high_168,
    MAX(IF(w.inside_1h = 0, w.row_idx, NULL)) OVER wcum         AS _last_non_inside
  FROM ohlc_win1 w
  WINDOW
    w24  AS (PARTITION BY w.token_address ORDER BY w.price_timestamp ROWS BETWEEN 23  PRECEDING AND CURRENT ROW),
    w168 AS (PARTITION BY w.token_address ORDER BY w.price_timestamp ROWS BETWEEN 167 PRECEDING AND CURRENT ROW),
    w720 AS (PARTITION BY w.token_address ORDER BY w.price_timestamp ROWS BETWEEN 719 PRECEDING AND CURRENT ROW),
    wcum AS (PARTITION BY w.token_address ORDER BY w.price_timestamp ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
),

-- Third window pass: cumulative adl, adx smoothing, array percent-ranks.
ohlc_win3 AS (
  SELECT
    w.*,
    SUM(w.adl_component) OVER wcum                              AS adl,
    IF(w.row_idx >= 48,  AVG(w.dx_24h)  OVER w24,  NULL)       AS adx_24h,
    IF(w.row_idx >= 336, AVG(w.dx_168h) OVER w168, NULL)       AS adx_168h,
    CAST(LEAST(w.row_idx - COALESCE(w._last_non_inside, 0), 6) AS FLOAT64) AS consec_inside_6h,
    CASE WHEN (SELECT COUNT(1) FROM UNNEST(w._arr_atr_720) AS v WHERE v >= 0) >= 720 THEN
      (SELECT SAFE_DIVIDE(COUNTIF(v <= w.atr_24h), COUNT(1)) FROM UNNEST(w._arr_atr_720) AS v WHERE v >= 0)
    END                                                        AS squeeze_pctile_720h,
    CASE WHEN (SELECT COUNT(1) FROM UNNEST(w._arr_range_24) AS v WHERE v >= 0) >= 24 THEN
      (SELECT SAFE_DIVIDE(COUNTIF(v <= w.range_1h), COUNT(1)) FROM UNNEST(w._arr_range_24) AS v WHERE v >= 0)
    END                                                        AS nr_pctrank_24h,
    CASE WHEN ARRAY_LENGTH(w._arr_high_168) >= 168 THEN
      (ARRAY_LENGTH(w._arr_high_168) - 1
        - (SELECT MAX(off) FROM UNNEST(w._arr_high_168) AS v WITH OFFSET off
             WHERE v = (SELECT MAX(v2) FROM UNNEST(w._arr_high_168) AS v2 WHERE v2 >= 0))
      ) / 168.0
    END                                                        AS bars_since_true_high_168h
  FROM ohlc_win2 w
  WINDOW
    w24  AS (PARTITION BY w.token_address ORDER BY w.price_timestamp ROWS BETWEEN 23  PRECEDING AND CURRENT ROW),
    w168 AS (PARTITION BY w.token_address ORDER BY w.price_timestamp ROWS BETWEEN 167 PRECEDING AND CURRENT ROW),
    wcum AS (PARTITION BY w.token_address ORDER BY w.price_timestamp ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
),

-- Fourth window pass: adl slope over the cumulative flow line.
ohlc_win4 AS (
  SELECT
    w.*,
    IF(w.row_idx >= 168, SAFE_DIVIDE(
        COVAR_POP(w.adl, CAST(w.row_idx AS FLOAT64)) OVER w168,
        NULLIF(VAR_POP(CAST(w.row_idx AS FLOAT64)) OVER w168, 0.0)), NULL) AS ad_slope_168h
  FROM ohlc_win3 w
  WINDOW
    w168 AS (PARTITION BY w.token_address ORDER BY w.price_timestamp ROWS BETWEEN 167 PRECEDING AND CURRENT ROW)
),

-- Fifth window pass: ad/price divergence (z720 of ad_slope minus z720 of slope).
ohlc_win5 AS (
  SELECT
    w.*,
    IF(w.row_idx >= 720,
        SAFE_DIVIDE(w.ad_slope_168h - AVG(w.ad_slope_168h) OVER w720, NULLIF(STDDEV_SAMP(w.ad_slope_168h) OVER w720, 0.0))
      - SAFE_DIVIDE(w._trend_slope_7d_ohlc - AVG(w._trend_slope_7d_ohlc) OVER w720, NULLIF(STDDEV_SAMP(w._trend_slope_7d_ohlc) OVER w720, 0.0)),
      NULL)                                                     AS ad_price_diverge_168h
  FROM ohlc_win4 w
  WINDOW
    w720 AS (PARTITION BY w.token_address ORDER BY w.price_timestamp ROWS BETWEEN 719 PRECEDING AND CURRENT ROW)
)

-- Shipped per-token OHLC feature columns.
SELECT
  token_address,
  price_timestamp,
  CAST(clv_1h AS FLOAT64) AS clv_1h,
  CAST(clv_mean_24h AS FLOAT64) AS clv_mean_24h,
  CAST(clv_mean_168h AS FLOAT64) AS clv_mean_168h,
  CAST(flow_imbalance_24h AS FLOAT64) AS flow_imbalance_24h,
  CAST(flow_imbalance_168h AS FLOAT64) AS flow_imbalance_168h,
  CAST(ad_slope_168h AS FLOAT64) AS ad_slope_168h,
  CAST(ad_price_diverge_168h AS FLOAT64) AS ad_price_diverge_168h,
  CAST(mfi_24h AS FLOAT64) AS mfi_24h,
  CAST(vwap_dist_24h AS FLOAT64) AS vwap_dist_24h,
  CAST(vwap_dist_168h AS FLOAT64) AS vwap_dist_168h,
  CAST(wick_asym_24h AS FLOAT64) AS wick_asym_24h,
  CAST(atr_24h AS FLOAT64) AS atr_24h,
  CAST(atr_168h AS FLOAT64) AS atr_168h,
  CAST(SAFE_DIVIDE(atr_24h, NULLIF(atr_168h, 0.0)) AS FLOAT64) AS atr_ratio_24_168,
  CAST(parkinson_rv_24h AS FLOAT64) AS parkinson_rv_24h,
  CAST(parkinson_rv_7d AS FLOAT64) AS parkinson_rv_7d,
  CAST(range_z_24h AS FLOAT64) AS range_z_24h,
  CAST(squeeze_pctile_720h AS FLOAT64) AS squeeze_pctile_720h,
  CAST(nr_pctrank_24h AS FLOAT64) AS nr_pctrank_24h,
  CAST(consec_inside_6h AS FLOAT64) AS consec_inside_6h,
  CAST(dist_to_true_high_24h AS FLOAT64) AS dist_to_true_high_24h,
  CAST(dist_to_true_high_168h AS FLOAT64) AS dist_to_true_high_168h,
  CAST(dist_to_true_high_720h AS FLOAT64) AS dist_to_true_high_720h,
  CAST(dist_to_true_low_24h AS FLOAT64) AS dist_to_true_low_24h,
  CAST(dist_to_true_low_168h AS FLOAT64) AS dist_to_true_low_168h,
  CAST(true_breakout_high_24h AS FLOAT64) AS true_breakout_high_24h,
  CAST(true_breakout_low_24h AS FLOAT64) AS true_breakout_low_24h,
  CAST(true_range_pos_168h AS FLOAT64) AS true_range_pos_168h,
  CAST(bars_since_true_high_168h AS FLOAT64) AS bars_since_true_high_168h,
  CAST(zero_range_frac_24h AS FLOAT64) AS zero_range_frac_24h,
  CAST(cs_spread_24h_bps AS FLOAT64) AS cs_spread_24h_bps,
  CAST(cs_spread_z_168h AS FLOAT64) AS cs_spread_z_168h,
  CAST(range_impact_24h AS FLOAT64) AS range_impact_24h,
  CAST(adx_24h AS FLOAT64) AS adx_24h,
  CAST(adx_168h AS FLOAT64) AS adx_168h,
  CAST(di_diff_24h AS FLOAT64) AS di_diff_24h,
  CAST(choppiness_168h AS FLOAT64) AS choppiness_168h,
  CAST(vortex_24h AS FLOAT64) AS vortex_24h,
  CAST(er_24h AS FLOAT64) AS er_24h,
  CAST(er_168h AS FLOAT64) AS er_168h,
  CAST(gk_rv_24h AS FLOAT64) AS gk_rv_24h,
  CAST(rs_rv_24h AS FLOAT64) AS rs_rv_24h,
  CAST(gap_abs_mean_24h AS FLOAT64) AS gap_abs_mean_24h,
  CAST(max_gap_168h AS FLOAT64) AS max_gap_168h,
  CAST(uw_frac_mean_24h AS FLOAT64) AS uw_frac_mean_24h,
  CAST(lw_frac_mean_24h AS FLOAT64) AS lw_frac_mean_24h,
  CAST(max_uw_frac_24h AS FLOAT64) AS max_uw_frac_24h,
  CAST(body_frac_mean_24h AS FLOAT64) AS body_frac_mean_24h,
  CAST(ar_spread_24h_bps AS FLOAT64) AS ar_spread_24h_bps
FROM ohlc_win5
