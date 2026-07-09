{{ config(
    materialized = 'table'
) }}

-- ============================================================================
-- Per-token information-structure features (novel-features spec 2026-07-08):
--   A1 idiosyncratic momentum (beta residuals), A3 dormancy/reawakening,
--   B1 VPIN proxy, B2 jump decomposition, plus _var_* helpers for B3 herding.
-- Materialized as a table so the 60d (w1440) rolling covariance/correlation and
-- the other per-token rolling windows are planned on their own, keeping the
-- view within BigQuery's query-planning complexity limit.
--
-- Full per-token history from 20m_cv_prod_72_7d_before (price/volume/logret_1h/
-- std_ret_24h/volume_z_24h/cumret_7d), joined to the meme index (rl_meme_index),
-- the SOL series (cv_btc_sol_1h), and cumret_30d (rl_token_horizon_features).
-- dvol_1h = price * volume (§1.1 proxy). Keyed (token_address, price_timestamp).
-- ============================================================================
WITH sol_series AS (
  SELECT
    ts_hour AS price_timestamp,
    SAFE_CAST(logret_1h AS FLOAT64) AS sol_logret_1h,
    SAFE_CAST(cumret_7d AS FLOAT64) AS sol_cumret_7d
  FROM {{ ref('cv_btc_sol_1h') }}
  WHERE token_address = 'So11111111111111111111111111111111111111112'
  QUALIFY ROW_NUMBER() OVER (PARTITION BY ts_hour ORDER BY ts_hour) = 1
),

-- raw per-bar volume (72_7d_before only exposes volume aggregates, not the bar);
-- dvol_1h = price * volume, matching the view's dollar-volume construction.
vol_series AS (
  SELECT
    address,
    datetime,
    MAX(SAFE_CAST(volume AS FLOAT64)) AS volume
  FROM {{ ref('20m_cv_prod_filled_hours') }}
  GROUP BY address, datetime
),

i0 AS (
  SELECT
    t.token_address,
    t.ts_hour AS price_timestamp,
    SAFE_CAST(t.price AS FLOAT64)  AS price,
    SAFE_CAST(t.price AS FLOAT64) * v.volume AS dvol_1h,
    COALESCE(SAFE_CAST(t.logret_1h AS FLOAT64), 0.0) AS logret_1h,
    SAFE_CAST(t.std_ret_24h AS FLOAT64)  AS std_ret_24h,
    SAFE_CAST(t.volume_z_24h AS FLOAT64) AS volume_z_24h,
    SAFE_CAST(t.cumret_7d AS FLOAT64)    AS cumret_7d,
    th.cumret_30d,
    mi.idx_logret_1h,
    mi.idx_cumret_7d,
    mi.idx_cumret_30d,
    sol.sol_logret_1h,
    sol.sol_cumret_7d,
    ROW_NUMBER() OVER (PARTITION BY t.token_address ORDER BY t.ts_hour) AS row_idx,
    LAG(COALESCE(SAFE_CAST(t.logret_1h AS FLOAT64), 0.0)) OVER (
      PARTITION BY t.token_address ORDER BY t.ts_hour
    ) AS prev_r
  FROM {{ ref('20m_cv_prod_72_7d_before') }} t
  LEFT JOIN {{ ref('rl_token_horizon_features') }} th
    ON t.token_address = th.token_address AND t.ts_hour = th.price_timestamp
  LEFT JOIN {{ ref('rl_meme_index') }} mi
    ON t.ts_hour = mi.price_timestamp
  LEFT JOIN sol_series sol
    ON t.ts_hour = sol.price_timestamp
  LEFT JOIN vol_series v
    ON t.token_address = v.address AND t.ts_hour = v.datetime
),

-- per-bar helpers: bulk-classification toxicity, jump terms, hot flag
i1 AS (
  SELECT
    i0.*,
    -- B1: one-sided dollar imbalance. 2*sigmoid(1.702 z) - 1 == tanh(0.851 z).
    ABS(TANH(0.851 * SAFE_DIVIDE(logret_1h, NULLIF(std_ret_24h, 0.0)))) * dvol_1h AS tox_1h,
    -- B2: realized-var / bipower / downside-var per-bar terms
    logret_1h * logret_1h                       AS r2,
    ABS(logret_1h) * ABS(prev_r)                AS bp_term,
    IF(logret_1h < 0, logret_1h * logret_1h, 0.0) AS down_r2,
    -- A3: dormancy activity flag
    IF(volume_z_24h >= 2.0, 1, 0)               AS hot_1h
  FROM i0
),

-- rolling window aggregates
i2 AS (
  SELECT
    i1.*,
    -- A1 betas (winsorized in the final projection) and idio vol share
    IF(row_idx >= 1440, SAFE_DIVIDE(
        COVAR_POP(logret_1h, sol_logret_1h) OVER w1440,
        NULLIF(VAR_POP(sol_logret_1h) OVER w1440, 0.0)), NULL) AS beta_sol_raw,
    IF(row_idx >= 1440, SAFE_DIVIDE(
        COVAR_POP(logret_1h, idx_logret_1h) OVER w1440,
        NULLIF(VAR_POP(idx_logret_1h) OVER w1440, 0.0)), NULL) AS beta_idx_raw,
    IF(row_idx >= 1440, CORR(logret_1h, idx_logret_1h) OVER w1440, NULL) AS corr_idx,
    -- B1 VPIN proxy
    IF(row_idx >= 24,  SAFE_DIVIDE(SUM(tox_1h) OVER w24,  NULLIF(SUM(dvol_1h) OVER w24,  0.0)), NULL) AS vpin_24h,
    IF(row_idx >= 168, SAFE_DIVIDE(SUM(tox_1h) OVER w168, NULLIF(SUM(dvol_1h) OVER w168, 0.0)), NULL) AS vpin_168h,
    -- B2 jump / downside decomposition (bpv scaled by pi/2)
    IF(row_idx >= 25,  SAFE_DIVIDE(GREATEST(SUM(r2) OVER w24  - 1.5707963267948966 * SUM(bp_term) OVER w24,  0.0), NULLIF(SUM(r2) OVER w24,  0.0)), NULL) AS jump_share_24h,
    IF(row_idx >= 169, SAFE_DIVIDE(GREATEST(SUM(r2) OVER w168 - 1.5707963267948966 * SUM(bp_term) OVER w168, 0.0), NULLIF(SUM(r2) OVER w168, 0.0)), NULL) AS jump_share_168h,
    IF(row_idx >= 24,  SAFE_DIVIDE(SUM(down_r2) OVER w24, NULLIF(SUM(r2) OVER w24, 0.0)), NULL) AS downside_semivar_share_24h,
    -- B3 per-token rolling variance (view averages these cross-sectionally)
    IF(row_idx >= 168, VAR_POP(logret_1h) OVER w168, NULL) AS var_168h,
    IF(row_idx >= 720, VAR_POP(logret_1h) OVER w720, NULL) AS var_720h,
    -- A3 last hot bar
    MAX(IF(hot_1h = 1, row_idx, NULL)) OVER (
      PARTITION BY token_address ORDER BY price_timestamp
      ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) AS last_hot_rn
  FROM i1
  WINDOW
    w24   AS (PARTITION BY token_address ORDER BY price_timestamp ROWS BETWEEN 23   PRECEDING AND CURRENT ROW),
    w168  AS (PARTITION BY token_address ORDER BY price_timestamp ROWS BETWEEN 167  PRECEDING AND CURRENT ROW),
    w720  AS (PARTITION BY token_address ORDER BY price_timestamp ROWS BETWEEN 719  PRECEDING AND CURRENT ROW),
    w1440 AS (PARTITION BY token_address ORDER BY price_timestamp ROWS BETWEEN 1439 PRECEDING AND CURRENT ROW)
)

SELECT
  token_address,
  price_timestamp,
  -- A1 idiosyncratic momentum (betas winsorized to [-2, 5] per §3)
  CAST(LEAST(GREATEST(beta_sol_raw, -2.0), 5.0) AS FLOAT64) AS beta_sol_60d,
  CAST(LEAST(GREATEST(beta_idx_raw, -2.0), 5.0) AS FLOAT64) AS beta_idx_60d,
  CAST(cumret_7d  - LEAST(GREATEST(beta_sol_raw, -2.0), 5.0) * sol_cumret_7d  AS FLOAT64) AS idio_mom_sol_7d,
  CAST(cumret_7d  - LEAST(GREATEST(beta_idx_raw, -2.0), 5.0) * idx_cumret_7d  AS FLOAT64) AS idio_mom_idx_7d,
  CAST(cumret_30d - LEAST(GREATEST(beta_idx_raw, -2.0), 5.0) * idx_cumret_30d AS FLOAT64) AS idio_mom_idx_30d,
  CAST(LEAST(GREATEST(1.0 - POW(corr_idx, 2), 0.0), 1.0) AS FLOAT64) AS idio_vol_share_60d,
  -- A3 dormancy / reawakening
  CAST(SAFE.LN(1.0 + (row_idx - COALESCE(last_hot_rn, 0)) / 24.0) AS FLOAT64) AS dormancy_days_log,
  CAST(SAFE.LN(1.0 + (row_idx - COALESCE(last_hot_rn, 0)) / 24.0) * GREATEST(volume_z_24h, 0.0) AS FLOAT64) AS reawakening_score,
  -- B1 VPIN
  CAST(vpin_24h AS FLOAT64)  AS vpin_24h,
  CAST(vpin_168h AS FLOAT64) AS vpin_168h,
  CAST(IF(row_idx >= 720, SAFE_DIVIDE(
      vpin_24h - AVG(vpin_24h) OVER w720,
      NULLIF(STDDEV_SAMP(vpin_24h) OVER w720, 0.0)), NULL) AS FLOAT64) AS vpin_z_30d,
  -- B2 jump decomposition
  CAST(jump_share_24h AS FLOAT64)  AS jump_share_24h,
  CAST(jump_share_168h AS FLOAT64) AS jump_share_168h,
  CAST(downside_semivar_share_24h AS FLOAT64) AS downside_semivar_share_24h,
  -- B3 helpers (not shipped by the view; averaged into herding_ratio_*)
  CAST(var_168h AS FLOAT64) AS _var_168h,
  CAST(var_720h AS FLOAT64) AS _var_720h
FROM i2
WINDOW w720 AS (PARTITION BY token_address ORDER BY price_timestamp ROWS BETWEEN 719 PRECEDING AND CURRENT ROW)
