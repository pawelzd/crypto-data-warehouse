{% macro rl_prod_asset_features_sql(bars_model, trade_size_usd=250.0) %}

-- One calculation path is shared by tokens, SOL, and BTC. The final serving
-- view selects only its contract, keeping unused training feature families out.
WITH lags AS (
  SELECT
    b.*,
    LEAD(price) OVER asset_order AS next_price,
    LAG(price) OVER asset_order AS previous_price,
    LAG(volume) OVER asset_order AS previous_volume,
    LAG(volume, 24) OVER asset_order AS volume_24h_ago,
    ROW_NUMBER() OVER asset_order AS row_number
  FROM {{ ref(bars_model) }} b
  WINDOW asset_order AS (
    PARTITION BY token_address ORDER BY price_timestamp
  )
),

returns AS (
  SELECT
    l.*,
    SAFE_DIVIDE(price, previous_price) - 1.0 AS ret_1h_raw,
    SAFE.LOG(SAFE_DIVIDE(price, previous_price)) AS logret_1h_raw,
    COALESCE(volume, 0.0) * COALESCE(price, 0.0) AS dollar_volume_h,
    LEAST(GREATEST(SAFE_DIVIDE(volume - previous_volume, NULLIF(previous_volume, 0.0)), -10.0), 10.0) AS volume_ret_1h_raw,
    LEAST(GREATEST(SAFE_DIVIDE(volume - volume_24h_ago, NULLIF(volume_24h_ago, 0.0)), -10.0), 10.0) AS volume_ret_24h_raw
  FROM lags l
),

return_lags AS (
  SELECT
    r.*,
    LAG(ret_1h_raw) OVER (
      PARTITION BY token_address ORDER BY price_timestamp
    ) AS previous_ret_1h
  FROM returns r
),

rolling AS (
  SELECT
    r.*,
    AVG(ret_1h_raw) OVER w24 AS mean_ret_24h_raw,
    AVG(ret_1h_raw) OVER w72 AS mean_ret_72h_raw,
    AVG(ret_1h_raw) OVER w168 AS mean_ret_168h_raw,
    STDDEV_SAMP(ret_1h_raw) OVER w24 AS std_ret_24h_raw,
    STDDEV_SAMP(ret_1h_raw) OVER w72 AS std_ret_72h_raw,
    STDDEV_SAMP(ret_1h_raw) OVER w168 AS std_ret_168h_raw,
    SQRT(SUM(POW(COALESCE(logret_1h_raw, 0.0), 2)) OVER w24) * SQRT(24.0) AS rv_24h_raw,
    SQRT(SUM(POW(COALESCE(logret_1h_raw, 0.0), 2)) OVER w168) * SQRT(24.0) AS rv_7d_raw,
    SAFE_DIVIDE(AVG(ret_1h_raw) OVER w24, NULLIF(STDDEV_SAMP(ret_1h_raw) OVER w24, 0.0)) * SQRT(24.0) AS sharpe_24h_raw,
    SAFE_DIVIDE(AVG(ret_1h_raw) OVER w168, NULLIF(STDDEV_SAMP(ret_1h_raw) OVER w168, 0.0)) * SQRT(24.0) AS sharpe_7d_raw,
    SAFE_DIVIDE(ret_1h_raw - AVG(ret_1h_raw) OVER w24, NULLIF(STDDEV_SAMP(ret_1h_raw) OVER w24, 0.0)) AS ret_z_24h_raw,
    EXP(SUM(COALESCE(logret_1h_raw, 0.0)) OVER w24) - 1.0 AS cumret_24h_raw,
    EXP(SUM(COALESCE(logret_1h_raw, 0.0)) OVER w168) - 1.0 AS cumret_7d_raw,
    AVG(price) OVER w6 AS sma_6h,
    AVG(price) OVER w12 AS sma_12h,
    AVG(price) OVER w24 AS sma_24h,
    AVG(price) OVER w26 AS sma_26h,
    AVG(price) OVER w72 AS sma_72h,
    AVG(price) OVER w168 AS sma_168h,
    STDDEV_SAMP(price) OVER w24 AS price_std_24h,
    MIN(price) OVER w24 AS price_low_24h,
    MAX(price) OVER w24 AS price_high_24h,
    MAX(price) OVER w168 AS price_high_168h,
    MAX(price) OVER w24_prior AS prior_high_24h,
    MIN(price) OVER w24_prior AS prior_low_24h,
    AVG(GREATEST(ret_1h_raw, 0.0)) OVER w14 AS avg_gain_14,
    AVG(GREATEST(-ret_1h_raw, 0.0)) OVER w14 AS avg_loss_14,
    -- Preserve the training pipeline's inclusive RANGE semantics. Its named
    -- 6h/24h/168h volume windows include both endpoints, so they contain
    -- 7/25/169 hourly observations on the complete hourly grid. These are
    -- intentionally different from the return windows below.
    SUM(volume) OVER wv6 AS volume_sum_6h,
    SUM(volume) OVER wv24 AS volume_sum_24h,
    SUM(volume) OVER wv168 AS volume_sum_168h,
    AVG(volume) OVER wv24 AS volume_mean_24h,
    AVG(volume) OVER wv168 AS volume_mean_168h,
    STDDEV_SAMP(volume) OVER wv24 AS volume_std_24h,
    STDDEV_SAMP(volume) OVER wv168 AS volume_std_168h,
    COUNT(*) OVER wv24 AS volume_n_24h,
    SUM(dollar_volume_h) OVER w24 AS dollar_vol_24h_raw,
    STDDEV_SAMP(dollar_volume_h) OVER w24 AS dollar_volume_std_24h,
    AVG(dollar_volume_h) OVER w24 AS dollar_volume_mean_24h,
    AVG(SAFE_DIVIDE(ABS(COALESCE(ret_1h_raw, 0.0)), NULLIF(dollar_volume_h, 0.0))) OVER w24 AS dollar_amihud_24h_raw,
    COVAR_SAMP(COALESCE(ret_1h_raw, 0.0), previous_ret_1h) OVER w24 AS ret_lag_cov_24h,
    COUNTIF(ret_1h_raw IS NOT NULL) OVER w168 AS valid_returns_168h
  FROM return_lags r
  WINDOW
    w6 AS (PARTITION BY token_address ORDER BY price_timestamp ROWS BETWEEN 5 PRECEDING AND CURRENT ROW),
    w12 AS (PARTITION BY token_address ORDER BY price_timestamp ROWS BETWEEN 11 PRECEDING AND CURRENT ROW),
    w14 AS (PARTITION BY token_address ORDER BY price_timestamp ROWS BETWEEN 13 PRECEDING AND CURRENT ROW),
    w24 AS (PARTITION BY token_address ORDER BY price_timestamp ROWS BETWEEN 23 PRECEDING AND CURRENT ROW),
    w24_prior AS (PARTITION BY token_address ORDER BY price_timestamp ROWS BETWEEN 24 PRECEDING AND 1 PRECEDING),
    w26 AS (PARTITION BY token_address ORDER BY price_timestamp ROWS BETWEEN 25 PRECEDING AND CURRENT ROW),
    w72 AS (PARTITION BY token_address ORDER BY price_timestamp ROWS BETWEEN 71 PRECEDING AND CURRENT ROW),
    w168 AS (PARTITION BY token_address ORDER BY price_timestamp ROWS BETWEEN 167 PRECEDING AND CURRENT ROW),
    wv6 AS (
      PARTITION BY token_address ORDER BY UNIX_SECONDS(price_timestamp)
      RANGE BETWEEN 21600 PRECEDING AND CURRENT ROW
    ),
    wv24 AS (
      PARTITION BY token_address ORDER BY UNIX_SECONDS(price_timestamp)
      RANGE BETWEEN 86400 PRECEDING AND CURRENT ROW
    ),
    wv168 AS (
      PARTITION BY token_address ORDER BY UNIX_SECONDS(price_timestamp)
      RANGE BETWEEN 604800 PRECEDING AND CURRENT ROW
    )
),

stationary AS (
  SELECT
    r.*,
    COALESCE(ret_1h_raw, 0.0) AS ret_1h,
    COALESCE(logret_1h_raw, 0.0) AS logret_1h,
    COALESCE(mean_ret_24h_raw, 0.0) AS mean_ret_24h,
    COALESCE(mean_ret_72h_raw, 0.0) AS mean_ret_72h,
    COALESCE(mean_ret_168h_raw, 0.0) AS mean_ret_168h,
    COALESCE(std_ret_24h_raw, 0.0) AS std_ret_24h,
    COALESCE(std_ret_72h_raw, 0.0) AS std_ret_72h,
    COALESCE(std_ret_168h_raw, 0.0) AS std_ret_168h,
    COALESCE(rv_24h_raw, 0.0) AS rv_24h,
    COALESCE(rv_7d_raw, 0.0) AS rv_7d,
    COALESCE(sharpe_24h_raw, 0.0) AS sharpe_24h,
    COALESCE(sharpe_7d_raw, 0.0) AS sharpe_7d,
    COALESCE(ret_z_24h_raw, 0.0) AS ret_z_24h,
    COALESCE(cumret_24h_raw, 0.0) AS cumret_24h,
    COALESCE(cumret_7d_raw, 0.0) AS cumret_7d,
    COALESCE(SAFE_DIVIDE(price, NULLIF(sma_6h, 0.0)) - 1.0, 0.0) AS dist_to_sma_6h,
    COALESCE(SAFE_DIVIDE(price, NULLIF(sma_12h, 0.0)) - 1.0, 0.0) AS dist_to_sma_12h,
    COALESCE(SAFE_DIVIDE(price, NULLIF(sma_24h, 0.0)) - 1.0, 0.0) AS dist_to_sma_24h,
    COALESCE(SAFE_DIVIDE(price, NULLIF(sma_72h, 0.0)) - 1.0, 0.0) AS dist_to_sma_72h,
    COALESCE(SAFE_DIVIDE(price, NULLIF(sma_168h, 0.0)) - 1.0, 0.0) AS dist_to_sma_168h,
    COALESCE(SAFE_DIVIDE(price - AVG(price) OVER w24, NULLIF(price_std_24h, 0.0)), 0.0) AS price_z_24h,
    COALESCE(SAFE_DIVIDE(price - price_low_24h, NULLIF(price_high_24h - price_low_24h, 0.0)), 0.0) AS pct_in_range_24h,
    COALESCE(SAFE_DIVIDE(price, NULLIF(price_high_24h, 0.0)) - 1.0, 0.0) AS dist_to_high_24h,
    COALESCE(SAFE_DIVIDE(price, NULLIF(price_low_24h, 0.0)) - 1.0, 0.0) AS dist_to_low_24h,
    CAST(IF(price > prior_high_24h, 1.0, 0.0) AS FLOAT64) AS breakout_high_24h,
    CAST(IF(price < prior_low_24h, 1.0, 0.0) AS FLOAT64) AS breakout_low_24h,
    COALESCE(SAFE_DIVIDE(price, NULLIF(price_high_168h, 0.0)) - 1.0, 0.0) AS drawdown_7d,
    CASE
      WHEN avg_loss_14 IS NULL OR avg_loss_14 = 0.0 THEN 100.0
      ELSE 100.0 - 100.0 / (1.0 + COALESCE(SAFE_DIVIDE(avg_gain_14, avg_loss_14), 0.0))
    END AS rsi_14,
    COALESCE(volume_ret_1h_raw, 0.0) AS volume_ret_1h,
    COALESCE(volume_ret_24h_raw, 0.0) AS volume_ret_24h,
    COALESCE(SAFE_DIVIDE(volume_std_24h, NULLIF(volume_mean_24h, 0.0)), 0.0) AS volume_cv_24h,
    COALESCE(SAFE_DIVIDE(volume_std_168h, NULLIF(volume_mean_168h, 0.0)), 0.0) AS volume_cv_168h,
    COALESCE(SAFE_DIVIDE(volume, NULLIF(SAFE_DIVIDE(volume_sum_24h - volume, NULLIF(volume_n_24h - 1, 0)), 0.0)), 0.0) AS volume_spike_ratio_24h_excl,
    COALESCE(SAFE_DIVIDE(volume - volume_mean_24h, NULLIF(volume_std_24h, 0.0)), 0.0) AS volume_z_24h,
    COALESCE(SAFE_DIVIDE(volume_sum_6h - volume_sum_24h, NULLIF(volume_sum_24h, 0.0)), 0.0) AS volume_accel_6v24,
    COALESCE(SAFE_DIVIDE(volume_sum_24h - volume_sum_168h, NULLIF(volume_sum_168h, 0.0)), 0.0) AS volume_accel_24v168,
    COALESCE(SAFE_DIVIDE(dollar_volume_std_24h, NULLIF(dollar_volume_mean_24h, 0.0)), 0.0) AS volume_cv_24h_dollar,
    COALESCE(dollar_vol_24h_raw, 0.0) AS dollar_vol_24h,
    COALESCE(dollar_amihud_24h_raw, 0.0) AS dollar_amihud_24h,
    CAST(IF(ret_lag_cov_24h < 0.0, 2.0 * SQRT(-ret_lag_cov_24h) * 10000.0, 0.0) AS FLOAT64) AS effective_spread_24h_bps,
    CAST(valid_returns_168h = 168 AS BOOL) AS has_168h,
    SUM(COALESCE(logret_1h_raw, 0.0)) OVER (
      PARTITION BY token_address ORDER BY price_timestamp
      ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) AS log_price_relative
  FROM rolling r
  WINDOW w24 AS (
    PARTITION BY token_address ORDER BY price_timestamp
    ROWS BETWEEN 23 PRECEDING AND CURRENT ROW
  )
),

trend_inputs AS (
  SELECT
    s.*,
    ROW_NUMBER() OVER (
      PARTITION BY token_address ORDER BY price_timestamp
    ) AS trend_row_number
  FROM stationary s
),

features AS (
  SELECT
    t.*,
    COALESCE(dist_to_sma_6h - dist_to_sma_24h, 0.0) AS sma_diff_fast_slow,
    sharpe_24h - sharpe_7d AS sharpe_delta,
    COALESCE(SAFE_DIVIDE(rsi_14 * SAFE_DIVIDE(rv_24h, NULLIF(rv_7d, 0.0)), 100.0), 0.0) AS rsi_vol_interaction,
    sma_12h - sma_26h AS macd_sma_12_26h,
    CASE WHEN COUNT(*) OVER w720 = 720 THEN log_price_relative - MAX(log_price_relative) OVER w720 END AS log_gap_from_30d_high,
    CASE WHEN COUNT(*) OVER w720 = 720 THEN log_price_relative - MIN(log_price_relative) OVER w720 END AS log_gap_to_30d_low,
    CASE WHEN COUNT(*) OVER w168 = 168 THEN SAFE_DIVIDE(
      COVAR_POP(log_price_relative, CAST(trend_row_number AS FLOAT64)) OVER w168,
      NULLIF(VAR_POP(CAST(trend_row_number AS FLOAT64)) OVER w168, 0.0)
    ) END AS trend_slope_7d,
    CASE WHEN COUNT(*) OVER w720 = 720 THEN SAFE_DIVIDE(
      COVAR_POP(log_price_relative, CAST(trend_row_number AS FLOAT64)) OVER w720,
      NULLIF(VAR_POP(CAST(trend_row_number AS FLOAT64)) OVER w720, 0.0)
    ) END AS trend_slope_30d,
    CASE WHEN COUNT(*) OVER w168 = 168 THEN POW(COALESCE(CORR(log_price_relative, CAST(trend_row_number AS FLOAT64)) OVER w168, 0.0), 2) END AS trend_r2_7d,
    CASE WHEN COUNT(*) OVER w720 = 720 THEN POW(COALESCE(CORR(log_price_relative, CAST(trend_row_number AS FLOAT64)) OVER w720, 0.0), 2) END AS trend_r2_30d
  FROM trend_inputs t
  WINDOW
    w168 AS (PARTITION BY token_address ORDER BY price_timestamp ROWS BETWEEN 167 PRECEDING AND CURRENT ROW),
    w720 AS (PARTITION BY token_address ORDER BY price_timestamp ROWS BETWEEN 719 PRECEDING AND CURRENT ROW)
),

costs AS (
  SELECT
    f.*,
    LEAST(CAST(EXP(
      -0.371
      + 0.573 * LN(GREATEST(CAST({{ trade_size_usd }} AS FLOAT64), 1.0))
      + 0.424 * LN(GREATEST(CAST({{ trade_size_usd }} AS FLOAT64) / GREATEST(dollar_vol_24h, 1.0), 1e-10))
      + 0.218 * LN(GREATEST(dollar_vol_24h / GREATEST(COALESCE(mktcap, 0.0), 1.0), 1e-10))
      + 0.149 * LN(GREATEST(dollar_vol_24h, 1.0))
      - 0.069 * LN(GREATEST(COALESCE(mktcap, 0.0), 1.0))
    ) / CAST({{ trade_size_usd }} AS FLOAT64) * 10000.0 AS FLOAT64), 500.0) AS fee_bps_default,
    CAST(CASE
      WHEN CAST({{ trade_size_usd }} AS FLOAT64) / GREATEST(dollar_vol_24h, 1.0) > 0.005 THEN 300.0
      WHEN CAST({{ trade_size_usd }} AS FLOAT64) / GREATEST(dollar_vol_24h, 1.0) > 0.0005 THEN 50.0
      ELSE 10.0
    END AS FLOAT64) AS slippage_bps_proxy
  FROM features f
)

SELECT * FROM costs
{% endmacro %}
