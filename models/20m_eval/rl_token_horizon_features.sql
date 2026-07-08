{{ config(
    materialized = 'table'
) }}

-- ============================================================================
-- Block A: token rotation / lifecycle long-horizon features (spec 2026-07-07).
-- Materialized as a table so the 90d (w2160) and unbounded ATH windows are
-- planned on their own, keeping rl_inference_features_next_open_v within
-- BigQuery's query-planning complexity limit (mirrors rl_ohlc_candle_features).
--
-- Computed over the FULL per-token hourly history from 20m_cv_prod_72_7d_before
-- (NOT the has_full_lookback-filtered eval_dataset) so the ATH and long windows
-- see every bar and warm-up gating by row_idx is accurate. Reuses the exact
-- existing price / logret series and recipes: arithmetic drawdown/dist_to_sma
-- (price/MAX-1, price/AVG-1), EXP-sum-logret cumret, and the log-price-rel gap
-- construction used by the existing log_gap_from_30d_high. log_gap_from_ath uses
-- the same log-price-rel basis (equivalent to LN(price/MAX price) on this
-- gap-free forward-filled series) so the §5.4 nesting invariant holds exactly.
-- Keyed (token_address, price_timestamp); LEFT-joined into the view by that key.
-- ============================================================================
WITH h0 AS (
  SELECT
    token_address,
    ts_hour AS price_timestamp,
    SAFE_CAST(price AS FLOAT64) AS price,
    COALESCE(SAFE_CAST(logret_1h AS FLOAT64), 0.0) AS logret_1h,
    ROW_NUMBER() OVER (PARTITION BY token_address ORDER BY ts_hour) AS row_idx
  FROM {{ ref('20m_cv_prod_72_7d_before') }}
),

h1 AS (
  SELECT
    h0.*,
    IF(row_idx >= 336, EXP(SUM(logret_1h) OVER w336) - 1, NULL) AS cumret_14d,
    IF(row_idx >= 720, EXP(SUM(logret_1h) OVER w720) - 1, NULL) AS cumret_30d,
    IF(row_idx >= 336, SAFE_DIVIDE(price, NULLIF(MAX(price) OVER w336, 0.0)) - 1, NULL) AS drawdown_14d,
    IF(row_idx >= 720, SAFE_DIVIDE(price, NULLIF(MAX(price) OVER w720, 0.0)) - 1, NULL) AS drawdown_30d,
    IF(row_idx >= 336, SAFE_DIVIDE(price, NULLIF(AVG(price) OVER w336, 0.0)) - 1, NULL) AS dist_to_sma_336h,
    IF(row_idx >= 720, SAFE_DIVIDE(price, NULLIF(AVG(price) OVER w720, 0.0)) - 1, NULL) AS dist_to_sma_720h,
    SUM(logret_1h) OVER wcum AS _log_price_rel,
    MAX(price) OVER wcum AS _run_max_price
  FROM h0
  WINDOW
    w336 AS (PARTITION BY token_address ORDER BY price_timestamp ROWS BETWEEN 335 PRECEDING AND CURRENT ROW),
    w720 AS (PARTITION BY token_address ORDER BY price_timestamp ROWS BETWEEN 719 PRECEDING AND CURRENT ROW),
    wcum AS (PARTITION BY token_address ORDER BY price_timestamp ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
),

h2 AS (
  SELECT
    h1.*,
    IF(row_idx >= 2160, _log_price_rel - MAX(_log_price_rel) OVER w2160, NULL) AS log_gap_from_90d_high,
    _log_price_rel - MAX(_log_price_rel) OVER wcum AS log_gap_from_ath,
    IF(price >= _run_max_price, 1, 0) AS is_new_high
  FROM h1
  WINDOW
    w2160 AS (PARTITION BY token_address ORDER BY price_timestamp ROWS BETWEEN 2159 PRECEDING AND CURRENT ROW),
    wcum  AS (PARTITION BY token_address ORDER BY price_timestamp ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
),

h3 AS (
  SELECT
    h2.*,
    MAX(IF(is_new_high = 1, row_idx, NULL)) OVER (
      PARTITION BY token_address ORDER BY price_timestamp
      ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) AS last_ath_rn
  FROM h2
)

SELECT
  token_address,
  price_timestamp,
  CAST(cumret_14d AS FLOAT64) AS cumret_14d,
  CAST(cumret_30d AS FLOAT64) AS cumret_30d,
  CAST(drawdown_14d AS FLOAT64) AS drawdown_14d,
  CAST(drawdown_30d AS FLOAT64) AS drawdown_30d,
  CAST(dist_to_sma_336h AS FLOAT64) AS dist_to_sma_336h,
  CAST(dist_to_sma_720h AS FLOAT64) AS dist_to_sma_720h,
  CAST(log_gap_from_90d_high AS FLOAT64) AS log_gap_from_90d_high,
  CAST(log_gap_from_ath AS FLOAT64) AS log_gap_from_ath,
  -- bars_since_ath / (rn - 1); 0 = at ATH now, ~1 = ATH at listing; NULL on bar 1
  CAST(IF(row_idx > 1, SAFE_DIVIDE(row_idx - last_ath_rn, row_idx - 1), NULL) AS FLOAT64) AS ath_recency_frac
FROM h3
