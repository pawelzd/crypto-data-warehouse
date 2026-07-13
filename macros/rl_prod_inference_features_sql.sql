{% macro rl_prod_inference_features_sql(asset_features_model, output_hours=none, min_mktcap=0, members_only=false, minimum_member_coverage=0.90) %}

WITH assets AS (
  SELECT * FROM {{ ref(asset_features_model) }}
),

scam_tokens AS (
  SELECT DISTINCT
    chain,
    token_address
  FROM {{ ref('scam_h_union') }}
),

candidate_tokens AS (
  SELECT a.*
  FROM assets a
  WHERE a.has_168h
    AND a.mktcap > {{ min_mktcap }}
    AND a.next_price IS NOT NULL
    AND NOT EXISTS (
      SELECT 1
      FROM scam_tokens s
      WHERE s.chain = 'sol'
        AND s.token_address = a.token_address
    )
),

membership AS (
  SELECT
    token_address,
    week_start,
    in_universe_pit
  FROM {{ ref('rl_prod_universe_membership_pit') }}
),

membership_weekly AS (
  SELECT
    week_start,
    COUNTIF(in_universe_pit) AS expected_member_count
  FROM membership
  GROUP BY week_start
),

annotated_tokens AS (
  SELECT
    c.*,
    COALESCE(m.in_universe_pit, FALSE) AS in_universe_pit
  FROM candidate_tokens c
  LEFT JOIN membership m
    ON m.token_address = c.token_address
   AND m.week_start = DATE_TRUNC(DATE(c.price_timestamp), WEEK(MONDAY))
),

active_tokens AS (
  SELECT *
  FROM annotated_tokens
  WHERE in_universe_pit
),

universe_hourly AS (
  SELECT
    price_timestamp,
    CAST(COUNT(*) AS FLOAT64) AS univ_n_active,
    CAST(APPROX_QUANTILES(cumret_24h, 100)[OFFSET(50)] AS FLOAT64) AS univ_med_logret_24h,
    CAST(AVG(cumret_24h) AS FLOAT64) AS univ_mean_logret_24h,
    CAST(STDDEV_SAMP(cumret_24h) AS FLOAT64) AS univ_disp_logret_24h,
    CAST(APPROX_QUANTILES(cumret_7d, 100)[OFFSET(50)] AS FLOAT64) AS univ_med_logret_168h,
    CAST(AVG(IF(IF(ABS(cumret_24h) < 1e-12, 0.0, cumret_24h) > 0.0, 1.0, 0.0)) AS FLOAT64) AS univ_pct_pos_24h,
    CAST(AVG(IF(dist_to_sma_24h > 0.0, 1.0, 0.0)) AS FLOAT64) AS univ_pct_above_sma_24h,
    CAST(AVG(IF(dist_to_sma_168h > 0.0, 1.0, 0.0)) AS FLOAT64) AS univ_pct_above_sma_168h,
    CAST(AVG(IF(drawdown_7d <= -0.10, 1.0, 0.0)) AS FLOAT64) AS univ_pct_in_dd_gt_10pct,
    CAST(APPROX_QUANTILES(rv_24h, 100)[OFFSET(50)] AS FLOAT64) AS univ_med_rv_24h
  FROM active_tokens
  GROUP BY price_timestamp
),

universe_regime AS (
  SELECT
    u.*,
    CASE WHEN COUNT(*) OVER w720 = 720 THEN CAST(SAFE_DIVIDE(
      univ_med_rv_24h - AVG(univ_med_rv_24h) OVER w720,
      NULLIF(STDDEV_SAMP(univ_med_rv_24h) OVER w720, 0.0)
    ) AS FLOAT64) END AS univ_med_rv_24h_z_30d
  FROM universe_hourly u
  WINDOW w720 AS (
    ORDER BY price_timestamp ROWS BETWEEN 719 PRECEDING AND CURRENT ROW
  )
),

relative AS (
  SELECT
    token_address,
    price_timestamp,
    CAST(PERCENT_RANK() OVER (PARTITION BY price_timestamp ORDER BY cumret_7d) AS FLOAT64) AS rel_rank_cumret_7d,
    CAST(PERCENT_RANK() OVER (
      PARTITION BY price_timestamp
      ORDER BY IF(ABS(cumret_24h) < 1e-12, 0.0, cumret_24h)
    ) AS FLOAT64) AS rel_rank_cumret_24h,
    CAST(PERCENT_RANK() OVER (PARTITION BY price_timestamp ORDER BY dollar_vol_24h) AS FLOAT64) AS rel_rank_dollar_vol_24h
  FROM active_tokens
),

btc_contract AS (
  SELECT
    price_timestamp,
    btc_logret_1h AS logret_1h,
    btc_cumret_24h AS cumret_24h,
    btc_cumret_7d AS cumret_7d
  FROM {{ source('rl_prod_artifacts', 'btc_reference_contract_v1') }}
),

btc_live AS (
  SELECT
    price_timestamp,
    logret_1h,
    cumret_24h,
    cumret_7d
  FROM assets
  WHERE token_address = 'btcusdt'
),

btc AS (
  SELECT * FROM btc_contract
  UNION ALL
  SELECT l.*
  FROM btc_live l
  WHERE l.price_timestamp > (SELECT MAX(price_timestamp) FROM btc_contract)
),

sol AS (
  SELECT *
  FROM assets
  WHERE token_address = 'So11111111111111111111111111111111111111112'
),

contract AS (
  SELECT
    t.token_address,
    t.price_timestamp,
    t.price AS next_open,
    t.next_price AS next_next_open,
    t.fee_bps_default,
    t.slippage_bps_proxy,
    t.dollar_amihud_24h,
    t.dollar_vol_24h,
    t.mktcap,
    t.in_universe_pit,
    mw.expected_member_count,

    t.cumret_24h,
    t.cumret_7d,
    t.logret_1h,
    t.mean_ret_168h,
    t.mean_ret_72h,
    t.ret_1h,
    t.ret_z_24h,
    t.sharpe_24h,
    t.sharpe_7d,
    t.sharpe_delta,

    t.dist_to_sma_6h,
    t.dist_to_sma_12h,
    t.dist_to_sma_24h,
    t.dist_to_sma_72h,
    t.dist_to_sma_168h,
    t.sma_diff_fast_slow,
    t.trend_slope_7d,
    t.trend_slope_30d,
    t.trend_r2_7d,
    t.trend_r2_30d,

    t.breakout_high_24h,
    t.breakout_low_24h,
    t.dist_to_high_24h,
    t.dist_to_low_24h,
    t.drawdown_7d,
    t.log_gap_from_30d_high,
    t.log_gap_to_30d_low,
    t.pct_in_range_24h,
    t.price_z_24h,

    t.volume_ret_1h,
    t.volume_ret_24h,
    t.volume_z_24h,
    t.volume_cv_24h,
    t.volume_cv_168h,
    t.volume_cv_24h_dollar,
    t.volume_accel_6v24,
    t.volume_accel_24v168,
    t.volume_spike_ratio_24h_excl,

    t.rsi_14,
    t.rsi_vol_interaction,
    t.effective_spread_24h_bps,
    COALESCE(t.logret_1h - b.logret_1h, 0.0) AS spread_logret_1h,

    CAST(EXTRACT(HOUR FROM t.price_timestamp) AS FLOAT64) AS hour_of_day,
    SIN(2.0 * 3.141592653589793 * EXTRACT(HOUR FROM t.price_timestamp) / 24.0) AS sin_hour,
    COS(2.0 * 3.141592653589793 * EXTRACT(HOUR FROM t.price_timestamp) / 24.0) AS cos_hour,
    SIN(2.0 * 3.141592653589793 * EXTRACT(DAYOFWEEK FROM t.price_timestamp) / 7.0) AS sin_dow,
    COS(2.0 * 3.141592653589793 * EXTRACT(DAYOFWEEK FROM t.price_timestamp) / 7.0) AS cos_dow,
    CAST(EXTRACT(DAYOFWEEK FROM t.price_timestamp) AS FLOAT64) AS dow_1_sun_7_sat,

    r.rel_rank_cumret_7d,
    r.rel_rank_cumret_24h,
    r.rel_rank_dollar_vol_24h,
    CAST(t.cumret_7d - u.univ_med_logret_168h AS FLOAT64) AS rel_excess_cumret_7d,
    CAST(t.cumret_24h - u.univ_med_logret_24h AS FLOAT64) AS rel_excess_cumret_24h,
    CAST(t.cumret_7d - b.cumret_7d AS FLOAT64) AS rel_excess_vs_btc_7d,
    CAST(t.cumret_24h - b.cumret_24h AS FLOAT64) AS rel_excess_vs_btc_24h,

    u.univ_n_active,
    u.univ_med_logret_24h,
    u.univ_mean_logret_24h,
    u.univ_disp_logret_24h,
    u.univ_med_logret_168h,
    u.univ_pct_pos_24h,
    u.univ_pct_above_sma_24h,
    u.univ_pct_above_sma_168h,
    u.univ_pct_in_dd_gt_10pct,
    u.univ_med_rv_24h,
    u.univ_med_rv_24h_z_30d,

    s.ret_1h AS sol_ret_1h,
    s.logret_1h AS sol_logret_1h,
    s.mean_ret_24h AS sol_mean_ret_24h,
    s.mean_ret_72h AS sol_mean_ret_72h,
    s.mean_ret_168h AS sol_mean_ret_168h,
    s.std_ret_24h AS sol_std_ret_24h,
    s.std_ret_72h AS sol_std_ret_72h,
    s.std_ret_168h AS sol_std_ret_168h,
    s.rv_24h AS sol_rv_24h,
    s.rv_7d AS sol_rv_7d,
    s.sharpe_24h AS sol_sharpe_24h,
    s.sharpe_7d AS sol_sharpe_7d,
    s.sharpe_delta AS sol_sharpe_delta,
    s.ret_z_24h AS sol_ret_z_24h,
    s.cumret_24h AS sol_cumret_24h,
    s.cumret_7d AS sol_cumret_7d,
    s.macd_sma_12_26h AS sol_macd_sma_12_26h,
    s.dist_to_sma_6h AS sol_dist_to_sma_6h,
    s.dist_to_sma_12h AS sol_dist_to_sma_12h,
    s.dist_to_sma_24h AS sol_dist_to_sma_24h,
    s.dist_to_sma_72h AS sol_dist_to_sma_72h,
    s.dist_to_sma_168h AS sol_dist_to_sma_168h,
    s.pct_in_range_24h AS sol_pct_in_range_24h,
    s.dist_to_high_24h AS sol_dist_to_high_24h,
    s.dist_to_low_24h AS sol_dist_to_low_24h,
    s.breakout_high_24h AS sol_breakout_high_24h,
    s.breakout_low_24h AS sol_breakout_low_24h,
    s.drawdown_7d AS sol_drawdown_7d,
    s.rsi_14 AS sol_rsi_14,
    SAFE_DIVIDE(s.rv_24h, NULLIF(s.rv_7d, 0.0)) AS sol_vol_ratio_24_7d,
    SIN(2.0 * 3.14 * EXTRACT(HOUR FROM t.price_timestamp) / 24.0) AS sol_sin_hour,
    COS(2.0 * 3.14 * EXTRACT(HOUR FROM t.price_timestamp) / 24.0) AS sol_cos_hour,
    SIN(2.0 * 3.14 * EXTRACT(DAYOFWEEK FROM t.price_timestamp) / 7.0) AS sol_sin_dow,
    COS(2.0 * 3.14 * EXTRACT(DAYOFWEEK FROM t.price_timestamp) / 7.0) AS sol_cos_dow
  FROM annotated_tokens t
  LEFT JOIN universe_regime u USING (price_timestamp)
  LEFT JOIN relative r USING (token_address, price_timestamp)
  INNER JOIN btc b USING (price_timestamp)
  LEFT JOIN sol s USING (price_timestamp)
  LEFT JOIN membership_weekly mw
    ON mw.week_start = DATE_TRUNC(DATE(t.price_timestamp), WEEK(MONDAY))
)

SELECT * EXCEPT(expected_member_count)
FROM contract
WHERE next_next_open IS NOT NULL
{% if members_only %}
  AND in_universe_pit
  AND univ_n_active >= expected_member_count * {{ minimum_member_coverage }}
{% endif %}
{% if output_hours is not none %}
  AND price_timestamp >= TIMESTAMP_SUB(
    TIMESTAMP_TRUNC(CURRENT_TIMESTAMP(), HOUR),
    INTERVAL {{ output_hours }} HOUR
  )
{% endif %}
{% endmacro %}
