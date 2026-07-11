{{ config(
    materialized = 'table'
) }}

-- ============================================================================
-- Momentum / pullback QUALITY features (spec 2026-07-11).
-- Nonlinear reorderings (rolling z-score, correlation, fraction, skew) of
-- existing channels -- they create cross-sectional ordering no current column
-- has (the explicit bet vs the rank-preserving adds that washed). Computed
-- per-token over the full 20m_cv_prod_72_7d_before history; materialized as a
-- table so the w720/w168 windows stay out of the base model's query plan.
-- Keyed (token_address, price_timestamp); LEFT-joined into the base model.
-- ============================================================================
WITH q0 AS (
  SELECT
    token_address,
    ts_hour AS price_timestamp,
    COALESCE(SAFE_CAST(logret_1h AS FLOAT64), 0.0) AS logret_1h,
    SAFE_CAST(drawdown_7d AS FLOAT64)  AS drawdown_7d,
    SAFE_CAST(volume_z_24h AS FLOAT64) AS volume_z_24h,
    SAFE_CAST(cumret_24h AS FLOAT64)   AS cumret_24h,
    SAFE_CAST(cumret_7d AS FLOAT64)    AS cumret_7d,
    ROW_NUMBER() OVER (PARTITION BY token_address ORDER BY ts_hour) AS row_idx
  FROM {{ ref('20m_cv_prod_72_7d_before') }}
),

q1 AS (
  SELECT
    q0.*,
    -- §3.1 pullback_z_720h: time-series z-score of drawdown_7d (abnormal depth)
    IF(row_idx >= 720, SAFE_DIVIDE(
        drawdown_7d - AVG(drawdown_7d) OVER w720,
        NULLIF(STDDEV(drawdown_7d) OVER w720, 0.0)), NULL) AS pullback_z_720h,
    -- §3.3 up_bar_frac_72h: grind vs spike
    IF(row_idx >= 72, AVG(IF(logret_1h > 0, 1.0, 0.0)) OVER w72, NULL) AS up_bar_frac_72h,
    -- §3.2 vol_mom_align_168h: population Pearson corr(logret_1h, volume_z_24h)
    IF(row_idx >= 168, SAFE_DIVIDE(
        AVG(logret_1h * volume_z_24h) OVER w168
          - AVG(logret_1h) OVER w168 * AVG(volume_z_24h) OVER w168,
        NULLIF(
          SQRT(GREATEST(VAR_POP(logret_1h) OVER w168, 0.0))
          * SQRT(GREATEST(VAR_POP(volume_z_24h) OVER w168, 0.0)), 0.0)),
      NULL) AS vol_mom_align_168h_raw,
    -- §3.5 ret_skew_168h: population moments (E[x^3] - 3 m E[x^2] + 2 m^3) / s^3
    AVG(logret_1h) OVER w168        AS _m1_168,
    AVG(POW(logret_1h, 2)) OVER w168 AS _m2_168,
    AVG(POW(logret_1h, 3)) OVER w168 AS _m3_168
  FROM q0
  WINDOW
    w72  AS (PARTITION BY token_address ORDER BY price_timestamp ROWS BETWEEN 71  PRECEDING AND CURRENT ROW),
    w168 AS (PARTITION BY token_address ORDER BY price_timestamp ROWS BETWEEN 167 PRECEDING AND CURRENT ROW),
    w720 AS (PARTITION BY token_address ORDER BY price_timestamp ROWS BETWEEN 719 PRECEDING AND CURRENT ROW)
)

SELECT
  token_address,
  price_timestamp,
  CAST(pullback_z_720h AS FLOAT64) AS pullback_z_720h,
  -- clip to [-1, 1] for float slop; NULL preserved through GREATEST/LEAST
  CAST(GREATEST(LEAST(vol_mom_align_168h_raw, 1.0), -1.0) AS FLOAT64) AS vol_mom_align_168h,
  CAST(up_bar_frac_72h AS FLOAT64) AS up_bar_frac_72h,
  -- §3.4 momentum_accel_24_168: recent pace minus trailing-7d average pace
  CAST(cumret_24h - cumret_7d * 24.0 / 168.0 AS FLOAT64) AS momentum_accel_24_168,
  -- §3.5 winsorized to [-10, 10]
  CAST(IF(row_idx >= 168,
      GREATEST(LEAST(
        SAFE_DIVIDE(
          _m3_168 - 3.0 * _m1_168 * _m2_168 + 2.0 * POW(_m1_168, 3),
          NULLIF(POW(SQRT(GREATEST(_m2_168 - _m1_168 * _m1_168, 0.0)), 3), 0.0)
        ), 10.0), -10.0),
      NULL) AS FLOAT64) AS ret_skew_168h
FROM q1
