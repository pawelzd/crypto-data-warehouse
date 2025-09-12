WITH base AS (
  -- Normalize to hourly bars (if multiple ticks per hour exist, take AVG close)
  SELECT
    tp.token_address,
    tp.type,
    TIMESTAMP_TRUNC(tp.first_acquired_timestamp, HOUR) AS ts_hour,
    AVG(CAST(tp.price_usd AS FLOAT64)) AS price
  FROM {{ ref('ml_tokens_prices_filtered_7daysbefore') }} AS tp
  WHERE tp.price_usd IS NOT NULL
  GROUP BY tp.token_address, ts_hour, tp.type
),
lags AS (
  SELECT
    b.token_address,
    b.ts_hour,
    b.price,
    b.type,
    LAG(b.price, 1) OVER (PARTITION BY b.token_address ORDER BY b.ts_hour) AS price_lag1,
  FROM base b
),
rets AS (
  SELECT
    l.token_address,
    l.ts_hour,
    l.price,
    l.type,
    l.price_lag1,
    SAFE_DIVIDE(l.price, l.price_lag1) - 1 AS ret_1h,
    SAFE.LOG(SAFE_DIVIDE(l.price, l.price_lag1)) AS logret_1h,
  FROM lags l
),
rets_with_lag AS (
  SELECT
    r.*,
    LAG(ret_1h, 1) OVER (PARTITION BY token_address ORDER BY ts_hour) AS ret_1h_lag1
  FROM rets r
),
roll AS (
  SELECT
    r.token_address,
    r.type,
    r.ts_hour,
    r.price,
    r.ret_1h,
    r.logret_1h,

    -- Rolling stats on returns
    AVG(r.ret_1h) OVER w4   AS mean_ret_4h,
    STDDEV_SAMP(r.ret_1h) OVER w4 AS std_ret_4h,
    AVG(r.ret_1h) OVER w6   AS mean_ret_6h,
    STDDEV_SAMP(r.ret_1h) OVER w6 AS std_ret_6h,
    AVG(r.ret_1h) OVER w12  AS mean_ret_12h,
    STDDEV_SAMP(r.ret_1h) OVER w12 AS std_ret_12h,
    AVG(r.ret_1h) OVER w24  AS mean_ret_24h,
    STDDEV_SAMP(r.ret_1h) OVER w24 AS std_ret_24h,
    AVG(r.ret_1h) OVER w72  AS mean_ret_72h,
    STDDEV_SAMP(r.ret_1h) OVER w72 AS std_ret_72h,
    AVG(r.ret_1h) OVER w168 AS mean_ret_168h,
    STDDEV_SAMP(r.ret_1h) OVER w168 AS std_ret_168h,

    -- Realized volatility (24h and 168h); sqrt(sum(logret^2)) * sqrt(k)
    SQRT(SUM(POW(COALESCE(r.logret_1h,0), 2)) OVER w24) * SQRT(24) AS rv_24h,
    SQRT(SUM(POW(COALESCE(r.logret_1h,0), 2)) OVER w168) * SQRT(24) AS rv_7d,
    SAFE_DIVIDE(AVG(r.ret_1h) OVER w4, NULLIF(STDDEV_SAMP(r.ret_1h) OVER w4,0)) * SQRT(6) AS sharpe_4h,
    SAFE_DIVIDE(AVG(r.ret_1h) OVER w6, NULLIF(STDDEV_SAMP(r.ret_1h) OVER w6,0)) * SQRT(4) AS sharpe_6h,
    SAFE_DIVIDE(AVG(r.ret_1h) OVER w12, NULLIF(STDDEV_SAMP(r.ret_1h) OVER w12,0)) * SQRT(2) AS sharpe_12h,
    SAFE_DIVIDE(AVG(r.ret_1h) OVER w24, NULLIF(STDDEV_SAMP(r.ret_1h) OVER w24,0)) AS sharpe_24h,
    SAFE_DIVIDE(AVG(r.ret_1h) OVER w168, NULLIF(STDDEV_SAMP(r.ret_1h) OVER w168,0)) * SQRT(24) AS sharpe_7d,

    -- Return z-score vs 24h vol
    SAFE_DIVIDE(r.ret_1h - AVG(r.ret_1h) OVER w24, NULLIF(STDDEV_SAMP(r.ret_1h) OVER w24,0)) AS ret_z_24h,

    -- Rolling cumulative return via log-returns (safer than product of 1+r)
    EXP(SUM(COALESCE(r.logret_1h,0)) OVER w4) - 1   AS cumret_4h,
    EXP(SUM(COALESCE(r.logret_1h,0)) OVER w6) - 1   AS cumret_6h,
    EXP(SUM(COALESCE(r.logret_1h,0)) OVER w12) - 1  AS cumret_12h,
    EXP(SUM(COALESCE(r.logret_1h,0)) OVER w24) - 1  AS cumret_24h,
    EXP(SUM(COALESCE(r.logret_1h,0)) OVER w168) - 1 AS cumret_7d,

    -- Simple MAs on price
    AVG(r.price) OVER w4   AS sma_4h,
    AVG(r.price) OVER w6   AS sma_6h,
    AVG(r.price) OVER w12  AS sma_12h,
    AVG(r.price) OVER w24  AS sma_24h,
    AVG(r.price) OVER w72  AS sma_72h,
    AVG(r.price) OVER w168 AS sma_168h,

    -- MACD-like using SMAs (fast−slow)
    (AVG(r.price) OVER w12) - (AVG(r.price) OVER w26) AS macd_sma_12_26h,

    -- Bollinger-style z-score
    (r.price - AVG(r.price) OVER w24) / NULLIF(STDDEV_SAMP(r.price) OVER w24,0) AS price_z_24h,

    -- Percent-of-range in window
    SAFE_DIVIDE(
      r.price - MIN(r.price) OVER w24,
      NULLIF(MAX(r.price) OVER w24 - MIN(r.price) OVER w24,0)
    ) AS pct_in_range_24h,

    -- Rolling extremes and distances
    r.price / NULLIF(MAX(r.price) OVER w24,0) - 1 AS dist_to_high_24h,
    r.price / NULLIF(MIN(r.price) OVER w24,0) - 1 AS dist_to_low_24h,

    -- Breakout flags vs prior window (exclude current row)
    CASE WHEN r.price > MAX(r.price) OVER (PARTITION BY r.token_address ORDER BY r.ts_hour
                                       ROWS BETWEEN 24 PRECEDING AND 1 PRECEDING)
          THEN 1 ELSE 0 END AS breakout_high_24h,
    CASE WHEN r.price < MIN(r.price) OVER (PARTITION BY r.token_address ORDER BY r.ts_hour
                                       ROWS BETWEEN 24 PRECEDING AND 1 PRECEDING)
          THEN 1 ELSE 0 END AS breakout_low_24h,

    -- Drawdown over 7 days
    r.price / NULLIF(MAX(r.price) OVER w168,0) - 1 AS drawdown_7d,

    -- Autocorrelation proxy: corr(ret_t, ret_{t-1}) in window
    CORR(r.ret_1h, r.ret_1h_lag1) OVER w72 AS acf1_72h,

    CASE WHEN COUNTIF(r.ret_1h IS NOT NULL) OVER w24  = 24  THEN 1 ELSE 0 END AS has_24h,
    CASE WHEN COUNTIF(r.ret_1h IS NOT NULL) OVER w72  = 72  THEN 1 ELSE 0 END AS has_72h,
    CASE WHEN COUNTIF(r.ret_1h IS NOT NULL) OVER w168 = 168 THEN 1 ELSE 0 END AS has_168h

  FROM rets_with_lag r
  WINDOW
    w4   AS (PARTITION BY r.token_address ORDER BY r.ts_hour ROWS BETWEEN 3  PRECEDING AND CURRENT ROW),
    w6   AS (PARTITION BY r.token_address ORDER BY r.ts_hour ROWS BETWEEN 5  PRECEDING AND CURRENT ROW),
    w12  AS (PARTITION BY r.token_address ORDER BY r.ts_hour ROWS BETWEEN 11 PRECEDING AND CURRENT ROW),
    w24  AS (PARTITION BY r.token_address ORDER BY r.ts_hour ROWS BETWEEN 23 PRECEDING AND CURRENT ROW),
    w26  AS (PARTITION BY r.token_address ORDER BY r.ts_hour ROWS BETWEEN 25 PRECEDING AND CURRENT ROW),
    w72  AS (PARTITION BY r.token_address ORDER BY r.ts_hour ROWS BETWEEN 71 PRECEDING AND CURRENT ROW),
    w168 AS (PARTITION BY r.token_address ORDER BY r.ts_hour ROWS BETWEEN 167 PRECEDING AND CURRENT ROW)
),
rsi AS (
  -- RSI(14) using SMA of gains/losses (Wilder EMA is slower in SQL)
  SELECT
    r.token_address,
    r.type,
    r.ts_hour,
    r.price,
    r.ret_1h, r.logret_1h,
    r.sharpe_4h, r.sharpe_6h, r.sharpe_12h,
    r.mean_ret_4h, r.std_ret_4h, r.mean_ret_6h, r.std_ret_6h, r.mean_ret_12h, r.std_ret_12h,
    r.mean_ret_24h, r.std_ret_24h, r.mean_ret_72h, r.std_ret_72h, r.mean_ret_168h, r.std_ret_168h,
    r.rv_24h, r.rv_7d, r.sharpe_24h, r.sharpe_7d, r.ret_z_24h, r.cumret_12h, r.cumret_6h, r.cumret_4h, r.cumret_24h, r.cumret_7d,
    r.sma_4h, r.sma_6h, r.sma_12h, r.sma_24h, r.sma_72h, r.sma_168h, r.macd_sma_12_26h,
    r.price_z_24h, r.pct_in_range_24h, r.dist_to_high_24h, r.dist_to_low_24h,
    r.breakout_high_24h, r.breakout_low_24h, r.drawdown_7d, r.acf1_72h,
    -- mom_6h, mom_12h, mom_24h, mom_72h, mom_168h,

    r.has_24h,
    r.has_72h,
    r.has_168h,
    -- RSI inputs
    GREATEST(r.ret_1h, 0) AS gain,
    GREATEST(-r.ret_1h, 0) AS loss,

    AVG(GREATEST(r.ret_1h, 0)) OVER (PARTITION BY r.token_address ORDER BY r.ts_hour ROWS BETWEEN 13 PRECEDING AND CURRENT ROW) AS avg_gain_14,
    AVG(GREATEST(-r.ret_1h, 0)) OVER (PARTITION BY r.token_address ORDER BY r.ts_hour ROWS BETWEEN 13 PRECEDING AND CURRENT ROW) AS avg_loss_14,
  FROM roll r
),

first_acquired_token AS (
  SELECT
    token_address,
    MIN(ts_hour) AS first_acquired_ts
  FROM base
  GROUP BY token_address
),

final AS (
  SELECT
    r.token_address,
    r.type,
    r.ts_hour,
    r.price,
    r.ret_1h,
    r.logret_1h,
    -- mom_6h, mom_12h, mom_24h, mom_72h, mom_168h,

    -- Vol & quality
    r.mean_ret_4h, r.std_ret_4h, r.mean_ret_6h, r.std_ret_6h, r.mean_ret_12h, r.std_ret_12h,
    r.mean_ret_24h, r.std_ret_24h, r.mean_ret_72h, r.std_ret_72h, r.mean_ret_168h, r.std_ret_168h,
    r.rv_24h, r.rv_7d, r.sharpe_24h, r.sharpe_7d, r.ret_z_24h, 
    r.cumret_4h, r.cumret_6h, r.cumret_12h, r.cumret_24h, r.cumret_7d,
    r.sharpe_4h, r.sharpe_6h, r.sharpe_12h,

    -- Trend/bands
    r.macd_sma_12_26h,
    r.price_z_24h, r.pct_in_range_24h,
    SAFE_DIVIDE(r.price, NULLIF(r.sma_4h, 0)) - 1 AS dist_to_sma_4h,
    SAFE_DIVIDE(r.price, NULLIF(r.sma_6h, 0)) - 1 AS dist_to_sma_6h,
    SAFE_DIVIDE(r.price, NULLIF(r.sma_12h, 0)) - 1 AS dist_to_sma_12h,
    SAFE_DIVIDE(r.price, NULLIF(r.sma_24h, 0)) - 1 AS dist_to_sma_24h,
    SAFE_DIVIDE(r.price, NULLIF(r.sma_72h, 0)) - 1 AS dist_to_sma_72h,
    SAFE_DIVIDE(r.price, NULLIF(r.sma_168h, 0)) - 1 AS dist_to_sma_168h,
    -- Breakouts & drawdowns
    r.dist_to_high_24h, r.dist_to_low_24h, r.breakout_high_24h, r.breakout_low_24h, r.drawdown_7d,

    -- RSI (SMA version)
    CASE
      WHEN r.avg_loss_14 IS NULL OR r.avg_loss_14 = 0 THEN 100
      ELSE 100 - 100 / (1 + SAFE_DIVIDE(r.avg_gain_14, NULLIF(r.avg_loss_14,0)))
    END AS rsi_14,

    -- Autocorr
    r.acf1_72h,

    -- Seasonality/time features
    EXTRACT(DAYOFWEEK FROM r.ts_hour) AS dow_1_sun_7_sat,
    EXTRACT(HOUR FROM r.ts_hour) AS hour_of_day,
    SIN(2*3.14*EXTRACT(HOUR FROM r.ts_hour)/24.0) AS sin_hour,
    COS(2*3.14*EXTRACT(HOUR FROM r.ts_hour)/24.0) AS cos_hour,
    SIN(2*3.14*CAST(EXTRACT(DAYOFWEEK FROM r.ts_hour) AS FLOAT64)/7.0) AS sin_dow,
    COS(2*3.14*CAST(EXTRACT(DAYOFWEEK FROM r.ts_hour) AS FLOAT64)/7.0) AS cos_dow,
    SAFE_DIVIDE(r.rv_24h, NULLIF(r.rv_7d, 0)) AS vol_ratio_24_7d,
    (r.sharpe_24h - r.sharpe_7d) AS sharpe_delta,
    (1 - r.has_24h)  AS miss_24h,
    (1 - r.has_72h)  AS miss_72h,
    (1 - r.has_168h) AS miss_168h
  FROM rsi r
)
SELECT f.*, TIMESTAMP_DIFF(fat.first_acquired_ts, f.ts_hour, DAY) AS tokens_age 
FROM final f
LEFT JOIN first_acquired_token fat
  ON f.token_address = fat.token_address

