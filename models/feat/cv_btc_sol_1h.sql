{{ config(
    schema='feat',
    materialized='view'
) }}
WITH base AS (
  SELECT
    address AS token_address,
    TIMESTAMP_TRUNC(datetime, HOUR) AS ts_hour,
    AVG(CAST(price AS FLOAT64)) AS price
  FROM {{ ref('20m_cv_prod_filled_hours') }}
  WHERE price IS NOT NULL
    AND address = 'So11111111111111111111111111111111111111112'
  --and datetime < '2025-05-01'
  GROUP BY address, ts_hour

  UNION ALL

  SELECT
    token_address,
    TIMESTAMP_TRUNC(price_timestamp, HOUR) AS ts_hour,
    AVG(CAST(close AS FLOAT64)) AS price
  FROM {{ ref('token_ohlcv') }}
  WHERE close IS NOT NULL
    AND token_address = 'btcusdt'
  GROUP BY token_address, ts_hour
),
lags AS (
  SELECT
    token_address,
    ts_hour,
    price,
    LAG(price, 1) OVER (PARTITION BY token_address ORDER BY ts_hour) AS price_lag1,
    LAG(price, 6) OVER (PARTITION BY token_address ORDER BY ts_hour) AS price_lag6,
    LAG(price, 12) OVER (PARTITION BY token_address ORDER BY ts_hour) AS price_lag12,
    LAG(price, 24) OVER (PARTITION BY token_address ORDER BY ts_hour) AS price_lag24,
    LAG(price, 72) OVER (PARTITION BY token_address ORDER BY ts_hour) AS price_lag72,
    LAG(price, 168) OVER (PARTITION BY token_address ORDER BY ts_hour) AS price_lag168
  FROM base
),
rets AS (
  SELECT
    token_address,
    ts_hour,
    price,
    price_lag1,
    SAFE_DIVIDE(price, price_lag1) - 1 AS ret_1h,
    SAFE.LOG(SAFE_DIVIDE(price, price_lag1)) AS logret_1h,

    -- multi-hour momentums (close/close_N − 1)
    -- SAFE_DIVIDE(price, price_lag6)  - 1 AS mom_6h,
    -- SAFE_DIVIDE(price, price_lag12) - 1 AS mom_12h,
    -- SAFE_DIVIDE(price, price_lag24) - 1 AS mom_24h,
    -- SAFE_DIVIDE(price, price_lag72) - 1 AS mom_72h,
    -- SAFE_DIVIDE(price, price_lag168) - 1 AS mom_168h
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
    ts_hour,
    price,
    ret_1h,
    logret_1h,

    -- Rolling stats on returns
    AVG(ret_1h) OVER w24  AS mean_ret_24h,
    STDDEV_SAMP(ret_1h) OVER w24 AS std_ret_24h,
    AVG(ret_1h) OVER w72  AS mean_ret_72h,
    STDDEV_SAMP(ret_1h) OVER w72 AS std_ret_72h,
    AVG(ret_1h) OVER w168 AS mean_ret_168h,
    STDDEV_SAMP(ret_1h) OVER w168 AS std_ret_168h,

    -- Realized volatility (24h and 168h); sqrt(sum(logret^2)) * sqrt(k)
    SQRT(SUM(POW(COALESCE(logret_1h,0), 2)) OVER w24) * SQRT(24) AS rv_24h,
    SQRT(SUM(POW(COALESCE(logret_1h,0), 2)) OVER w168) * SQRT(24) AS rv_7d,
    SAFE_DIVIDE(AVG(ret_1h) OVER w24, NULLIF(STDDEV_SAMP(ret_1h) OVER w24,0)) * SQRT(24) AS sharpe_24h,
    SAFE_DIVIDE(AVG(ret_1h) OVER w168, NULLIF(STDDEV_SAMP(ret_1h) OVER w168,0)) * SQRT(24) AS sharpe_7d,
    -- Return z-score vs 24h vol
    SAFE_DIVIDE(ret_1h - AVG(ret_1h) OVER w24, NULLIF(STDDEV_SAMP(ret_1h) OVER w24,0)) AS ret_z_24h,

    -- Rolling cumulative return via log-returns (safer than product of 1+r)
    EXP(SUM(COALESCE(logret_1h,0)) OVER w24) - 1  AS cumret_24h,
    EXP(SUM(COALESCE(logret_1h,0)) OVER w168) - 1 AS cumret_7d,

    -- Simple MAs on price
    AVG(price) OVER w6   AS sma_6h,
    AVG(price) OVER w12  AS sma_12h,
    AVG(price) OVER w24  AS sma_24h,
    AVG(price) OVER w72  AS sma_72h,
    AVG(price) OVER w168 AS sma_168h,

    -- MACD-like using SMAs (fast−slow)
    (AVG(price) OVER w12) - (AVG(price) OVER w26) AS macd_sma_12_26h,

    -- Bollinger-style z-score
    (price - AVG(price) OVER w24) / NULLIF(STDDEV_SAMP(price) OVER w24,0) AS price_z_24h,

    -- Percent-of-range in window
    SAFE_DIVIDE(
      price - MIN(price) OVER w24,
      NULLIF(MAX(price) OVER w24 - MIN(price) OVER w24,0)
    ) AS pct_in_range_24h,

    -- Rolling extremes and distances
    price / NULLIF(MAX(price) OVER w24,0) - 1 AS dist_to_high_24h,
    price / NULLIF(MIN(price) OVER w24,0) - 1 AS dist_to_low_24h,

    -- Breakout flags vs prior window (exclude current row)
    CASE WHEN price > MAX(price) OVER (PARTITION BY token_address ORDER BY ts_hour
                                       ROWS BETWEEN 24 PRECEDING AND 1 PRECEDING)
          THEN 1 ELSE 0 END AS breakout_high_24h,
    CASE WHEN price < MIN(price) OVER (PARTITION BY token_address ORDER BY ts_hour
                                       ROWS BETWEEN 24 PRECEDING AND 1 PRECEDING)
          THEN 1 ELSE 0 END AS breakout_low_24h,

    -- Drawdown over 7 days
    price / NULLIF(MAX(price) OVER w168,0) - 1 AS drawdown_7d,

    -- Autocorrelation proxy: corr(ret_t, ret_{t-1}) in window
    CORR(ret_1h, ret_1h_lag1) OVER w72 AS acf1_72h,
    CASE WHEN COUNTIF(ret_1h IS NOT NULL) OVER w24  = 24  THEN 1 ELSE 0 END AS has_24h,
    CASE WHEN COUNTIF(ret_1h IS NOT NULL) OVER w72  = 72  THEN 1 ELSE 0 END AS has_72h,
    CASE WHEN COUNTIF(ret_1h IS NOT NULL) OVER w168 = 168 THEN 1 ELSE 0 END AS has_168h
  
  FROM rets_with_lag
  WINDOW
    w6   AS (PARTITION BY token_address ORDER BY ts_hour ROWS BETWEEN 5  PRECEDING AND CURRENT ROW),
    w12  AS (PARTITION BY token_address ORDER BY ts_hour ROWS BETWEEN 11 PRECEDING AND CURRENT ROW),
    w24  AS (PARTITION BY token_address ORDER BY ts_hour ROWS BETWEEN 23 PRECEDING AND CURRENT ROW),
    w26  AS (PARTITION BY token_address ORDER BY ts_hour ROWS BETWEEN 25 PRECEDING AND CURRENT ROW),
    w72  AS (PARTITION BY token_address ORDER BY ts_hour ROWS BETWEEN 71 PRECEDING AND CURRENT ROW),
    w168 AS (PARTITION BY token_address ORDER BY ts_hour ROWS BETWEEN 167 PRECEDING AND CURRENT ROW)
),
rsi AS (
  -- RSI(14) using SMA of gains/losses (Wilder EMA is slower in SQL)
  SELECT
    token_address,
    ts_hour,
    price,
    ret_1h, logret_1h,
    mean_ret_24h, std_ret_24h, mean_ret_72h, std_ret_72h, mean_ret_168h, std_ret_168h,
    rv_24h, rv_7d, sharpe_24h, sharpe_7d, ret_z_24h, cumret_24h, cumret_7d,
    sma_6h, sma_12h, sma_24h, sma_72h, sma_168h, macd_sma_12_26h,
    price_z_24h, pct_in_range_24h, dist_to_high_24h, dist_to_low_24h,
    breakout_high_24h, breakout_low_24h, drawdown_7d, acf1_72h,
    -- mom_6h, mom_12h, mom_24h, mom_72h, mom_168h,

    has_24h,
    has_72h,
    has_168h,
    -- RSI inputs
    GREATEST(ret_1h, 0) AS gain,
    GREATEST(-ret_1h, 0) AS loss,

    AVG(GREATEST(ret_1h, 0)) OVER (PARTITION BY token_address ORDER BY ts_hour ROWS BETWEEN 13 PRECEDING AND CURRENT ROW) AS avg_gain_14,
    AVG(GREATEST(-ret_1h, 0)) OVER (PARTITION BY token_address ORDER BY ts_hour ROWS BETWEEN 13 PRECEDING AND CURRENT ROW) AS avg_loss_14,
  FROM roll
),

final AS (
  SELECT
    r.token_address,
    r.ts_hour,
    r.price,

    -- Core returns
    ret_1h,
    logret_1h,
    -- mom_6h, mom_12h, mom_24h, mom_72h, mom_168h,

    -- Vol & quality
    mean_ret_24h, std_ret_24h, mean_ret_72h, std_ret_72h, mean_ret_168h, std_ret_168h,
    rv_24h, rv_7d, sharpe_24h, sharpe_7d, ret_z_24h, cumret_24h, cumret_7d,

    -- Trend/bands
    sma_6h, sma_12h, sma_24h, sma_72h, sma_168h, macd_sma_12_26h,
    price_z_24h, pct_in_range_24h,
    SAFE_DIVIDE(price, NULLIF(sma_6h, 0)) - 1 AS dist_to_sma_6h,
    SAFE_DIVIDE(price, NULLIF(sma_12h, 0)) - 1 AS dist_to_sma_12h,
    SAFE_DIVIDE(price, NULLIF(sma_24h, 0)) - 1 AS dist_to_sma_24h,
    SAFE_DIVIDE(price, NULLIF(sma_72h, 0)) - 1 AS dist_to_sma_72h,
    SAFE_DIVIDE(price, NULLIF(sma_168h, 0)) - 1 AS dist_to_sma_168h,
    -- Breakouts & drawdowns
    dist_to_high_24h, dist_to_low_24h, breakout_high_24h, breakout_low_24h, drawdown_7d,

    -- RSI (SMA version)
    CASE
      WHEN avg_loss_14 IS NULL OR avg_loss_14 = 0 THEN 100
      ELSE 100 - 100 / (1 + SAFE_DIVIDE(avg_gain_14, NULLIF(avg_loss_14,0)))
    END AS rsi_14,

    -- Autocorr
    acf1_72h,

    -- Seasonality/time features
    EXTRACT(DAYOFWEEK FROM ts_hour) AS dow_1_sun_7_sat,
    EXTRACT(HOUR FROM ts_hour) AS hour_of_day,
    SIN(2*3.14*EXTRACT(HOUR FROM ts_hour)/24.0) AS sin_hour,
    COS(2*3.14*EXTRACT(HOUR FROM ts_hour)/24.0) AS cos_hour,
    SIN(2*3.14*CAST(EXTRACT(DAYOFWEEK FROM ts_hour) AS FLOAT64)/7.0) AS sin_dow,
    COS(2*3.14*CAST(EXTRACT(DAYOFWEEK FROM ts_hour) AS FLOAT64)/7.0) AS cos_dow,
    SAFE_DIVIDE(rv_24h, NULLIF(rv_7d, 0)) AS vol_ratio_24_7d,
    (sharpe_24h - sharpe_7d) AS sharpe_delta,
    (1 - has_24h)  AS miss_24h,
    (1 - has_72h)  AS miss_72h,
    (1 - has_168h) AS miss_168h
  FROM rsi r
)
SELECT * FROM final
ORDER BY token_address, ts_hour
