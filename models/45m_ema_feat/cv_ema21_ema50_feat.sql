

{% set horizon_hours = var('horizon_hours', 48) %}
{% set take_profit   = var('take_profit',   0.06) %}
{% set sr_recent_days = var('sr_recent_days', none) %}
{% set default_numeric_fallback = var('default_numeric_fallback', 0.0) %}
{% set dist_fallback = var('dist_fallback', 1.0) %}

WITH ohlc AS (
  SELECT
    v.token_chain_id AS token_address, v.price_timestamp AS ts_hour, v.open, v.high, v.low, v.close, v.volume, c.ema_21, c.ema_50,
    LAG(close) OVER w AS prev_close
  FROM {{ ref('token_ohlcv_view') }} v
  INNER JOIN {{ ref('cv_ema21_ema50_calc') }} c 
  ON c.token_chain_id = v.token_chain_id AND c.price_timestamp = v.price_timestamp
  WINDOW w AS (PARTITION BY v.token_chain_id ORDER BY v.price_timestamp)
),

tr AS (
  SELECT
    *,
    GREATEST(
      high - low,
      ABS(high - IFNULL(prev_close, close)),
      ABS(low  - IFNULL(prev_close, close))
    ) AS true_range
  FROM ohlc
),

atr AS (
  SELECT
    *,
    AVG(true_range) OVER (PARTITION BY token_address ORDER BY ts_hour ROWS BETWEEN 13 PRECEDING AND CURRENT ROW) AS atr14,
    AVG(close)      OVER (PARTITION BY token_address ORDER BY ts_hour ROWS BETWEEN 19 PRECEDING AND CURRENT ROW) AS bb_mid,
    STDDEV(close)   OVER (PARTITION BY token_address ORDER BY ts_hour ROWS BETWEEN 19 PRECEDING AND CURRENT ROW) AS bb_std
  FROM tr
),

crosses AS (
  SELECT
    *,
    LAG(ema_21) OVER w AS ema21_prev,
    LAG(ema_50) OVER w AS ema50_prev,
    CASE WHEN LAG(ema_21) OVER w <= LAG(ema_50) OVER w AND ema_21 > ema_50 THEN 1 ELSE 0 END AS ema_cross_up
  FROM atr
  WINDOW w AS (PARTITION BY token_address ORDER BY ts_hour)
),

features AS (
  SELECT
    token_address,
    ts_hour AS event_ts,
    close   AS entry_px,
    (ema_21 - ema_50) / NULLIF(ema_50, 0) AS ema_spread,
    ((ema_21 - ema_50) - LAG(ema_21 - ema_50, 6) OVER (PARTITION BY token_address ORDER BY ts_hour))
      / NULLIF(6 * ema_50, 0) AS ema_spread_slope,
    (close - ema_50) / NULLIF(ema_50, 0) AS price_ema50_dist,
    atr14 / NULLIF(close, 0) AS atr_pct,
    ((bb_mid + 2*bb_std) - (bb_mid - 2*bb_std)) / NULLIF(bb_mid, 0) AS bb_width,
    SAFE_DIVIDE(
      close - MIN(low) OVER (PARTITION BY token_address ORDER BY ts_hour ROWS BETWEEN 24 PRECEDING AND CURRENT ROW),
      NULLIF(
        MAX(high) OVER (PARTITION BY token_address ORDER BY ts_hour ROWS BETWEEN 24 PRECEDING AND CURRENT ROW) -
        MIN(low)  OVER (PARTITION BY token_address ORDER BY ts_hour ROWS BETWEEN 24 PRECEDING AND CURRENT ROW)
      ,0)
    ) AS pct_in_range,
    LOG10(
      NULLIF(
        SAFE_DIVIDE(
          SUM(true_range) OVER (PARTITION BY token_address ORDER BY ts_hour ROWS BETWEEN 24 PRECEDING AND CURRENT ROW),
          NULLIF(
            (MAX(high) OVER (PARTITION BY token_address ORDER BY ts_hour ROWS BETWEEN 24 PRECEDING AND CURRENT ROW) -
             MIN(low)  OVER (PARTITION BY token_address ORDER BY ts_hour ROWS BETWEEN 24 PRECEDING AND CURRENT ROW))
          ,0)
        )
      ,0)
    ) * 100 / LOG10(24.0) AS choppiness_index,
    volume / NULLIF(AVG(volume) OVER (PARTITION BY token_address ORDER BY ts_hour ROWS BETWEEN 13 PRECEDING AND CURRENT ROW), 0) AS volume_ratio,
    ABS(close - open) / NULLIF(high - low, 0) AS body_ratio
  FROM crosses
  WHERE ema_cross_up = 1
),

-- Time-aware daily S/R (no leakage)
-- sr_join AS (
--   SELECT
--     f.*,
--     MIN(CASE WHEN s.level >= f.entry_px THEN s.level END) AS nearest_res_above,
--     MAX(CASE WHEN s.level <= f.entry_px THEN s.level END) AS nearest_sup_below
--   FROM features f
--   LEFT JOIN {{ ref('cv_support_resistance_daily') }} s
--     ON s.token_address = f.token_address
--    AND DATE(s.last_touch) <= DATE(f.event_ts)
--    {% if sr_recent_days is not none %}
--    AND DATE(s.last_touch) >= DATE_SUB(DATE(f.event_ts), INTERVAL {{ sr_recent_days }} DAY)
--    {% endif %}
--   GROUP BY
--     f.token_address, f.event_ts, f.entry_px,
--     f.ema_spread, f.ema_spread_slope, f.price_ema50_dist,
--     f.atr_pct, f.bb_width, f.pct_in_range, f.choppiness_index,
--     f.volume_ratio, f.body_ratio
-- ),

-- features_final AS (
--   SELECT
--     *,
--     SAFE_DIVIDE(nearest_res_above - entry_px, entry_px) AS res_dist_pct,
--     SAFE_DIVIDE(entry_px - nearest_sup_below, entry_px) AS sup_dist_pct
--   FROM sr_join
-- ),



/* ---------- Imputation stats ---------- */
token_medians AS (
  SELECT
    token_address,
    APPROX_QUANTILES(ema_spread,        100)[OFFSET(50)] AS med_ema_spread,
    APPROX_QUANTILES(ema_spread_slope,  100)[OFFSET(50)] AS med_ema_spread_slope,
    APPROX_QUANTILES(price_ema50_dist,  100)[OFFSET(50)] AS med_price_ema50_dist,
    APPROX_QUANTILES(atr_pct,           100)[OFFSET(50)] AS med_atr_pct,
    APPROX_QUANTILES(bb_width,          100)[OFFSET(50)] AS med_bb_width,
    APPROX_QUANTILES(pct_in_range,      100)[OFFSET(50)] AS med_pct_in_range,
    APPROX_QUANTILES(choppiness_index,  100)[OFFSET(50)] AS med_choppiness_index,
    APPROX_QUANTILES(volume_ratio,      100)[OFFSET(50)] AS med_volume_ratio,
    APPROX_QUANTILES(body_ratio,        100)[OFFSET(50)] AS med_body_ratio,
    -- APPROX_QUANTILES(res_dist_pct,      100)[OFFSET(50)] AS med_res_dist_pct,
    -- APPROX_QUANTILES(sup_dist_pct,      100)[OFFSET(50)] AS med_sup_dist_pct
  FROM features
  GROUP BY token_address
),

global_medians AS (
  SELECT
    APPROX_QUANTILES(ema_spread,        100)[OFFSET(50)] AS med_ema_spread,
    APPROX_QUANTILES(ema_spread_slope,  100)[OFFSET(50)] AS med_ema_spread_slope,
    APPROX_QUANTILES(price_ema50_dist,  100)[OFFSET(50)] AS med_price_ema50_dist,
    APPROX_QUANTILES(atr_pct,           100)[OFFSET(50)] AS med_atr_pct,
    APPROX_QUANTILES(bb_width,          100)[OFFSET(50)] AS med_bb_width,
    APPROX_QUANTILES(pct_in_range,      100)[OFFSET(50)] AS med_pct_in_range,
    APPROX_QUANTILES(choppiness_index,  100)[OFFSET(50)] AS med_choppiness_index,
    APPROX_QUANTILES(volume_ratio,      100)[OFFSET(50)] AS med_volume_ratio,
    APPROX_QUANTILES(body_ratio,        100)[OFFSET(50)] AS med_body_ratio,
    -- APPROX_QUANTILES(res_dist_pct,      100)[OFFSET(50)] AS med_res_dist_pct,
    -- APPROX_QUANTILES(sup_dist_pct,      100)[OFFSET(50)] AS med_sup_dist_pct
  FROM features
),

final_imputed AS (
  SELECT
    f.token_address,
    f.event_ts,
    f.entry_px,
    COALESCE(f.ema_spread,        tm.med_ema_spread,        gm.med_ema_spread,        {{ default_numeric_fallback }}) AS ema_spread,
    COALESCE(f.ema_spread_slope,  tm.med_ema_spread_slope,  gm.med_ema_spread_slope,  {{ default_numeric_fallback }}) AS ema_spread_slope,
    COALESCE(f.price_ema50_dist,  tm.med_price_ema50_dist,  gm.med_price_ema50_dist,  {{ default_numeric_fallback }}) AS price_ema50_dist,
    COALESCE(f.atr_pct,           tm.med_atr_pct,           gm.med_atr_pct,           {{ default_numeric_fallback }}) AS atr_pct,
    COALESCE(f.bb_width,          tm.med_bb_width,          gm.med_bb_width,          {{ default_numeric_fallback }}) AS bb_width,
    COALESCE(f.pct_in_range,      tm.med_pct_in_range,      gm.med_pct_in_range,      {{ default_numeric_fallback }}) AS pct_in_range,
    COALESCE(f.choppiness_index,  tm.med_choppiness_index,  gm.med_choppiness_index,  {{ default_numeric_fallback }}) AS choppiness_index,
    COALESCE(f.volume_ratio,      tm.med_volume_ratio,      gm.med_volume_ratio,      {{ default_numeric_fallback }}) AS volume_ratio,
    COALESCE(f.body_ratio,        tm.med_body_ratio,        gm.med_body_ratio,        {{ default_numeric_fallback }}) AS body_ratio,
    -- COALESCE(f.res_dist_pct,      tm.med_res_dist_pct,      gm.med_res_dist_pct,      {{ dist_fallback }}) AS res_dist_pct,
    -- COALESCE(f.sup_dist_pct,      tm.med_sup_dist_pct,      gm.med_sup_dist_pct,      {{ dist_fallback }}) AS sup_dist_pct
  FROM features f
  LEFT JOIN token_medians  tm USING (token_address)
  CROSS JOIN global_medians gm
)

SELECT
  fi.*,
  -- td.sharpe_24h,
  -- td.breakout_high_24h,
  -- td.drawdown_7d,
  -- td.rv_24h,
  -- td.dist_to_high_24h, td.dist_to_low_24h,
  -- td.ret_over_rv_12h,
  -- td.btc_sharpe_24h,
  -- td.btc_dist_to_sma_24h,
  -- td.btc_ret_1h,
  -- td.spread_ret_1h,
  -- td.volume_accel_6v24,
  -- td.volume_spike_ratio_24h_excl,
  -- td.sin_hour, td.cos_hour,
  -- td.sin_dow, td.cos_dow,
  td.* EXCEPT(token_address, decision_ts)
FROM final_imputed fi
INNER JOIN {{ ref('cv_filter_72_training_dataset') }} td
  ON fi.token_address = td.token_address
 AND fi.event_ts     = td.decision_ts
ORDER BY token_address, event_ts
