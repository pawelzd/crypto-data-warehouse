{{ config(
    schema='gold_ml_coins_mon',
    materialized='table'
) }}
WITH base AS (
  SELECT
    token_address,
    monitoring_session_id,
    session_start,
    session_end,
    extended_start,
    extended_end,
    in_pre_extension,
    in_core_monitoring,
    in_post_extension,
    TIMESTAMP_TRUNC(price_timestamp, HOUR) AS ts_hour,
    AVG(CAST(price_usd AS FLOAT64)) AS price
  FROM {{ ref('price_filter_prep_ext_windows') }}
  WHERE price_usd IS NOT NULL
  GROUP BY
    token_address, monitoring_session_id, session_start, session_end,
    extended_start, extended_end, in_pre_extension, in_core_monitoring, in_post_extension,
    ts_hour
),

lags AS (
  SELECT
    token_address,
    monitoring_session_id,
    session_start,
    session_end,
    extended_start,
    extended_end,
    in_pre_extension,
    in_core_monitoring,
    in_post_extension,
    ts_hour,
    price,
    LAG(price, 1)   OVER (PARTITION BY token_address ORDER BY ts_hour) AS price_lag1,
    LAG(price, 6)   OVER (PARTITION BY token_address ORDER BY ts_hour) AS price_lag6,
    LAG(price, 12)  OVER (PARTITION BY token_address ORDER BY ts_hour) AS price_lag12,
    LAG(price, 24)  OVER (PARTITION BY token_address ORDER BY ts_hour) AS price_lag24,
    LAG(price, 72)  OVER (PARTITION BY token_address ORDER BY ts_hour) AS price_lag72,
    LAG(price, 168) OVER (PARTITION BY token_address ORDER BY ts_hour) AS price_lag168
  FROM base
),

rets AS (
  SELECT
    token_address,
    monitoring_session_id,
    session_start,
    session_end,
    extended_start,
    extended_end,
    in_pre_extension,
    in_core_monitoring,
    in_post_extension,
    ts_hour,
    price,
    price_lag1,
    SAFE_DIVIDE(price, price_lag1) - 1 AS ret_1h,
    CASE
      WHEN price > 0 AND price_lag1 > 0 THEN LOG(price) - LOG(price_lag1)
      ELSE NULL
    END AS logret_1h
  FROM lags
),

rets_with_lag AS (
  SELECT
    r.*,
    LAG(ret_1h, 1) OVER (PARTITION BY token_address ORDER BY ts_hour) AS ret_1h_lag1
  FROM rets r
),

roll AS (
  SELECT
    token_address,
    monitoring_session_id,
    session_start,
    session_end,
    extended_start,
    extended_end,
    in_pre_extension,
    in_core_monitoring,
    in_post_extension,
    ts_hour,
    price,
    ret_1h,
    logret_1h,

    AVG(ret_1h) OVER w24  AS mean_ret_24h,
    STDDEV_SAMP(ret_1h) OVER w24 AS std_ret_24h,
    AVG(ret_1h) OVER w72  AS mean_ret_72h,
    STDDEV_SAMP(ret_1h) OVER w72 AS std_ret_72h,
    AVG(ret_1h) OVER w168 AS mean_ret_168h,
    STDDEV_SAMP(ret_1h) OVER w168 AS std_ret_168h,

    SQRT(SUM(POW(COALESCE(logret_1h,0), 2)) OVER w24)  * SQRT(24) AS rv_24h,
    SQRT(SUM(POW(COALESCE(logret_1h,0), 2)) OVER w168) * SQRT(24) AS rv_7d,

    SAFE_DIVIDE(AVG(ret_1h) OVER w24,  NULLIF(STDDEV_SAMP(ret_1h) OVER w24, 0)) * SQRT(24) AS sharpe_24h,
    SAFE_DIVIDE(AVG(ret_1h) OVER w168, NULLIF(STDDEV_SAMP(ret_1h) OVER w168,0)) * SQRT(24) AS sharpe_7d,

    SAFE_DIVIDE(ret_1h - AVG(ret_1h) OVER w24, NULLIF(STDDEV_SAMP(ret_1h) OVER w24,0)) AS ret_z_24h,

    EXP(SUM(COALESCE(logret_1h,0)) OVER w24)  - 1 AS cumret_24h,
    EXP(SUM(COALESCE(logret_1h,0)) OVER w168) - 1 AS cumret_7d,

    AVG(price) OVER w6   AS sma_6h,
    AVG(price) OVER w12  AS sma_12h,
    AVG(price) OVER w24  AS sma_24h,
    AVG(price) OVER w72  AS sma_72h,
    AVG(price) OVER w168 AS sma_168h,

    (AVG(price) OVER w12) - (AVG(price) OVER w26) AS macd_sma_12_26h,

    (price - AVG(price) OVER w24) / NULLIF(STDDEV_SAMP(price) OVER w24,0) AS price_z_24h,

    SAFE_DIVIDE(
      price - MIN(price) OVER w24,
      NULLIF(MAX(price) OVER w24 - MIN(price) OVER w24, 0)
    ) AS pct_in_range_24h,

    price / NULLIF(MAX(price) OVER w24, 0) - 1 AS dist_to_high_24h,
    price / NULLIF(MIN(price) OVER w24, 0) - 1 AS dist_to_low_24h,
    -- Difference between fast and slow SMAs


    CASE WHEN price > MAX(price) OVER (PARTITION BY token_address ORDER BY ts_hour
                                       ROWS BETWEEN 24 PRECEDING AND 1 PRECEDING)
          THEN 1 ELSE 0 END AS breakout_high_24h,
    CASE WHEN price < MIN(price) OVER (PARTITION BY token_address ORDER BY ts_hour
                                       ROWS BETWEEN 24 PRECEDING AND 1 PRECEDING)
          THEN 1 ELSE 0 END AS breakout_low_24h,

    price / NULLIF(MAX(price) OVER w168, 0) - 1 AS drawdown_7d,

    CORR(ret_1h, ret_1h_lag1) OVER w72 AS acf1_72h,

    CASE WHEN COUNTIF(ret_1h IS NOT NULL) OVER w24  = 24  THEN 1 ELSE 0 END AS has_24h,
    CASE WHEN COUNTIF(ret_1h IS NOT NULL) OVER w72  = 72  THEN 1 ELSE 0 END AS has_72h,
    CASE WHEN COUNTIF(ret_1h IS NOT NULL) OVER w168 = 168 THEN 1 ELSE 0 END AS has_168h
  FROM rets_with_lag
  WINDOW
   
    w6   AS (PARTITION BY token_address ORDER BY ts_hour ROWS BETWEEN 5   PRECEDING AND CURRENT ROW),
    w12  AS (PARTITION BY token_address ORDER BY ts_hour ROWS BETWEEN 11  PRECEDING AND CURRENT ROW),
    w24  AS (PARTITION BY token_address ORDER BY ts_hour ROWS BETWEEN 23  PRECEDING AND CURRENT ROW),
    w26  AS (PARTITION BY token_address ORDER BY ts_hour ROWS BETWEEN 25  PRECEDING AND CURRENT ROW),
    w72  AS (PARTITION BY token_address ORDER BY ts_hour ROWS BETWEEN 71  PRECEDING AND CURRENT ROW),
    w168 AS (PARTITION BY token_address ORDER BY ts_hour ROWS BETWEEN 167 PRECEDING AND CURRENT ROW)
),

rsi AS (
  SELECT
    token_address,
    monitoring_session_id,
    session_start,
    session_end,
    extended_start,
    extended_end,
    in_pre_extension,
    in_core_monitoring,
    in_post_extension,
    ts_hour,
    price,
    ret_1h, logret_1h,
    mean_ret_24h, std_ret_24h, mean_ret_72h, std_ret_72h, mean_ret_168h, std_ret_168h,
    rv_24h, rv_7d, sharpe_24h, sharpe_7d, ret_z_24h, cumret_24h, cumret_7d,
    sma_6h, sma_12h, sma_24h, sma_72h, sma_168h, macd_sma_12_26h,
    price_z_24h, pct_in_range_24h, dist_to_high_24h, dist_to_low_24h,
    breakout_high_24h, breakout_low_24h, drawdown_7d, acf1_72h,


    has_24h, has_72h, has_168h,

    GREATEST(ret_1h, 0)  AS gain,
    GREATEST(-ret_1h, 0) AS loss,

    AVG(GREATEST(ret_1h, 0))  OVER (PARTITION BY token_address ORDER BY ts_hour ROWS BETWEEN 13 PRECEDING AND CURRENT ROW) AS avg_gain_14,
    AVG(GREATEST(-ret_1h, 0)) OVER (PARTITION BY token_address ORDER BY ts_hour ROWS BETWEEN 13 PRECEDING AND CURRENT ROW) AS avg_loss_14
  FROM roll
),

final AS (
  SELECT
    r.token_address,
    r.monitoring_session_id,
    r.session_start,
    r.session_end,
    r.extended_start,
    r.extended_end,
    r.in_pre_extension,
    r.in_core_monitoring,
    r.in_post_extension,

    r.ts_hour,
    r.price,
    -- Core returns
    COALESCE(r.ret_1h, 0)     AS ret_1h,
    COALESCE(r.logret_1h, 0)  AS logret_1h,

    -- Vol & quality
    COALESCE(r.mean_ret_24h, 0)   AS mean_ret_24h,
    COALESCE(r.std_ret_24h, 0)    AS std_ret_24h,
    COALESCE(r.mean_ret_72h, 0)   AS mean_ret_72h,
    COALESCE(r.std_ret_72h, 0)    AS std_ret_72h,
    COALESCE(r.mean_ret_168h, 0)  AS mean_ret_168h,
    COALESCE(r.std_ret_168h, 0)   AS std_ret_168h,
    COALESCE(r.rv_24h, 0)         AS rv_24h,
    COALESCE(r.rv_7d, 0)          AS rv_7d,
    COALESCE(r.sharpe_24h, 0)     AS sharpe_24h,
    COALESCE(r.sharpe_7d, 0)      AS sharpe_7d,
    COALESCE(r.ret_z_24h, 0)      AS ret_z_24h,
    COALESCE(r.cumret_24h, 0)     AS cumret_24h,
    COALESCE(r.cumret_7d, 0)      AS cumret_7d,

    -- Trend/bands
    COALESCE(r.sma_6h, 0)         AS sma_6h,
    COALESCE(r.sma_12h, 0)        AS sma_12h,
    COALESCE(r.sma_24h, 0)        AS sma_24h,
    COALESCE(r.sma_72h, 0)        AS sma_72h,
    COALESCE(r.sma_168h, 0)       AS sma_168h,
    COALESCE(r.macd_sma_12_26h, 0) AS macd_sma_12_26h,
    COALESCE(r.price_z_24h, 0)    AS price_z_24h,
    COALESCE(r.pct_in_range_24h, 0) AS pct_in_range_24h,

    COALESCE(SAFE_DIVIDE(r.price, NULLIF(r.sma_6h, 0))   - 1, 0) AS dist_to_sma_6h,
    COALESCE(SAFE_DIVIDE(r.price, NULLIF(r.sma_12h, 0))  - 1, 0) AS dist_to_sma_12h,
    COALESCE(SAFE_DIVIDE(r.price, NULLIF(r.sma_24h, 0))  - 1, 0) AS dist_to_sma_24h,
    COALESCE(SAFE_DIVIDE(r.price, NULLIF(r.sma_72h, 0))  - 1, 0) AS dist_to_sma_72h,
    COALESCE(SAFE_DIVIDE(r.price, NULLIF(r.sma_168h, 0)) - 1, 0) AS dist_to_sma_168h,

    -- Breakouts & drawdowns
    COALESCE(r.dist_to_high_24h, 0) AS dist_to_high_24h,
    COALESCE(r.dist_to_low_24h, 0)  AS dist_to_low_24h,
    r.breakout_high_24h,
    r.breakout_low_24h,
    COALESCE(r.drawdown_7d, 0)      AS drawdown_7d,
    r.has_168h,

    -- RSI (SMA version) – null-safe
    CASE
      WHEN r.avg_loss_14 IS NULL OR r.avg_loss_14 = 0 THEN 100
      ELSE 100 - 100 / (1 + COALESCE(SAFE_DIVIDE(r.avg_gain_14, NULLIF(r.avg_loss_14, 0)), 0))
    END AS rsi_14,

    -- Autocorr
    COALESCE(r.acf1_72h, 0) AS acf1_72h,

    -- Seasonality/time features
    EXTRACT(DAYOFWEEK FROM r.ts_hour) AS dow_1_sun_7_sat,
    EXTRACT(HOUR FROM r.ts_hour)      AS hour_of_day,
    SIN(2 * 3.141592653589793 * EXTRACT(HOUR FROM r.ts_hour) / 24.0) AS sin_hour,
    COS(2 * 3.141592653589793 * EXTRACT(HOUR FROM r.ts_hour) / 24.0) AS cos_hour,
    SIN(2 * 3.141592653589793 * CAST(EXTRACT(DAYOFWEEK FROM r.ts_hour) AS FLOAT64) / 7.0) AS sin_dow,
    COS(2 * 3.141592653589793 * CAST(EXTRACT(DAYOFWEEK FROM r.ts_hour) AS FLOAT64) / 7.0) AS cos_dow,

    COALESCE(SAFE_DIVIDE(r.rv_24h, NULLIF(r.rv_7d, 0)), 0) AS vol_ratio_24_7d,
    COALESCE(SAFE_DIVIDE(std_ret_24h, std_ret_72h), 0) AS vol_ratio_24_72,
    COALESCE(SAFE_DIVIDE(std_ret_72h, std_ret_168h), 0) AS vol_ratio_72_168,
    COALESCE((r.sharpe_24h - r.sharpe_7d), 0)              AS sharpe_delta,
    

    -- Missingness (keep as 0/1 masks)
    (1 - r.has_24h)  AS miss_24h,
    (1 - r.has_72h)  AS miss_72h,
    (1 - r.has_168h) AS miss_168h
  FROM rsi r
),

distances_and_slopes AS (
  SELECT
    f.*,
    -- fast vs slow distance
    COALESCE(f.dist_to_sma_6h - f.dist_to_sma_24h, 0) AS sma_diff_fast_slow,

    -- slopes over the last 24 hours (prior rows only via LAG)
    COALESCE( (f.dist_to_sma_6h  - LAG(f.dist_to_sma_6h,  24) OVER w) / 24, 0) AS sma6h_slope_24h,
    COALESCE( (f.dist_to_sma_12h - LAG(f.dist_to_sma_12h, 24) OVER w) / 24, 0) AS sma12h_slope_24h,

    -- RSI x volume-regime interaction
    COALESCE(SAFE_DIVIDE(f.rsi_14 * f.vol_ratio_24_7d, 100), 0) AS rsi_vol_interaction

    -- If/when you join BTC features into `final`, you can also add:
    -- COALESCE(f.ret_1h - f.btc_ret_1h, 0) AS spread_ret_1h,
    -- COALESCE(f.sharpe_24h - f.btc_sharpe_24h, 0) AS spread_sharpe_24h
  FROM final f
  WINDOW w AS (PARTITION BY f.token_address ORDER BY f.ts_hour)
)


SELECT *
FROM distances_and_slopes
ORDER BY token_address, ts_hour