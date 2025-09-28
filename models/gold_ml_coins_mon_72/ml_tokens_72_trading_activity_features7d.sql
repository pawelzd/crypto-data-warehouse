{{ config(
    schema='gold_ml_coins_mon_72',
    materialized='table'
) }}


WITH base AS (
  SELECT
    token_address,
    TIMESTAMP_TRUNC(first_acquired_timestamp, HOUR) AS ts_hour,
    SAFE_CAST(ABS(delta_buy_bal_1h) + ABS(delta_sell_bal_1h) AS FLOAT64) AS volume
  FROM {{ ref('fct__token_trading_activity') }}
),

supplies AS (
  SELECT
    token_address,
    SAFE_CAST(total_supply AS FLOAT64) AS total_supply
  FROM {{ ref('fct__token_market_metadata') }}
),

lags AS (
  SELECT
    token_address,
    ts_hour,
    volume,
    ROW_NUMBER() OVER (PARTITION BY token_address ORDER BY ts_hour) AS rn,
    LAG(volume, 1)  OVER (PARTITION BY token_address ORDER BY ts_hour) AS vol_lag_1h,
    LAG(volume, 24) OVER (PARTITION BY token_address ORDER BY ts_hour) AS vol_lag_24h
  FROM base
),

roll AS (
  SELECT
    token_address,
    ts_hour,
    rn,
    volume,
    SUM(volume) OVER w_6h   AS vol_sum_6h,
    SUM(volume) OVER w_24h  AS vol_sum_24h,
    SUM(volume) OVER w_168h AS vol_sum_168h,
    AVG(volume) OVER w_24h  AS vol_mean_24h,
    AVG(volume) OVER w_168h AS vol_mean_168h,
    STDDEV_SAMP(volume) OVER w_24h  AS vol_std_24h,
    STDDEV_SAMP(volume) OVER w_168h AS vol_std_168h,
    COUNT(*) OVER w_24h AS n_24h
  FROM lags
  WINDOW
    w_6h   AS (
      PARTITION BY token_address
      ORDER BY UNIX_SECONDS(ts_hour)
      RANGE BETWEEN 21600 PRECEDING AND CURRENT ROW
    ),
    w_24h  AS (
      PARTITION BY token_address
      ORDER BY UNIX_SECONDS(ts_hour)
      RANGE BETWEEN 86400 PRECEDING AND CURRENT ROW
    ),
    w_168h AS (
      PARTITION BY token_address
      ORDER BY UNIX_SECONDS(ts_hour)
      RANGE BETWEEN 604800 PRECEDING AND CURRENT ROW
    )
),

ema AS (
  SELECT
    r.*,
    0.75 AS alpha_fast,
    0.90 AS alpha_slow,
    POW(0.75, rn) AS a_fast_pow,
    POW(0.90, rn) AS a_slow_pow
  FROM roll r
),

ema_agg AS (
  SELECT
    e.*,
    SAFE_DIVIDE(
      SUM(volume * a_fast_pow) OVER w_unbounded,
      NULLIF(SUM(a_fast_pow) OVER w_unbounded, 0)
    ) AS vol_ema_fast,
    SAFE_DIVIDE(
      SUM(volume * a_slow_pow) OVER w_unbounded,
      NULLIF(SUM(a_slow_pow) OVER w_unbounded, 0)
    ) AS vol_ema_slow
  FROM ema e
  WINDOW
    w_unbounded AS (
      PARTITION BY token_address
      ORDER BY UNIX_SECONDS(ts_hour)
      ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    )
),

feats AS (
  SELECT
    l.token_address,
    l.ts_hour,
    s.total_supply,


    LEAST(GREATEST(SAFE_DIVIDE(l.volume - l.vol_lag_1h,  NULLIF(l.vol_lag_1h,  0)), -10), 10) AS vol_ret_1h,
    LEAST(GREATEST(SAFE_DIVIDE(l.volume - l.vol_lag_24h, NULLIF(l.vol_lag_24h, 0)), -10), 10) AS vol_ret_24h,

    LOG(1 + l.volume) AS log_volume,
    LOG(1 + SAFE_DIVIDE(l.volume, NULLIF(s.total_supply, 0))) AS log_volume_per_supply,

    LOG(1 + r.vol_mean_24h)  AS log_vol_mean_24h,
    LOG(1 + r.vol_mean_168h) AS log_vol_mean_168h,
    LOG(1 + SAFE_DIVIDE(r.vol_mean_24h,  NULLIF(s.total_supply, 0))) AS log_vol_mean_24h_per_supply,
    LOG(1 + SAFE_DIVIDE(r.vol_mean_168h, NULLIF(s.total_supply, 0))) AS log_vol_mean_168h_per_supply,



    SAFE_DIVIDE(r.vol_std_24h,  NULLIF(r.vol_mean_24h,  0)) AS vol_cv_24h,
    SAFE_DIVIDE(r.vol_std_168h, NULLIF(r.vol_mean_168h, 0)) AS vol_cv_168h,

    SAFE_DIVIDE(
      l.volume,
      NULLIF(SAFE_DIVIDE(r.vol_sum_24h - l.volume, NULLIF(r.n_24h - 1, 0)), 0)
    ) AS vol_spike_ratio_24h_excl,
    SAFE_DIVIDE(l.volume - r.vol_mean_24h, NULLIF(r.vol_std_24h, 0)) AS vol_z_24h,

    SAFE_DIVIDE(r.vol_sum_6h  - r.vol_sum_24h,  NULLIF(r.vol_sum_24h,  0)) AS vol_accel_6v24,
    SAFE_DIVIDE(r.vol_sum_24h - r.vol_sum_168h, NULLIF(r.vol_sum_168h, 0)) AS vol_accel_24v168,

    -- Note: use numeric ORDER BY for RANGE here too
    CORR(l.volume, l.vol_lag_1h) OVER (
      PARTITION BY l.token_address
      ORDER BY UNIX_SECONDS(l.ts_hour)
      RANGE BETWEEN 86400 PRECEDING AND CURRENT ROW
    ) AS vol_autocorr1_24h,

    ea.vol_ema_fast,
    ea.vol_ema_slow,

    EXTRACT(HOUR FROM l.ts_hour) AS hour_of_day,
    IF(EXTRACT(DAYOFWEEK FROM l.ts_hour) IN (1,7), 1, 0) AS is_weekend

  FROM lags l
  JOIN roll r
    ON r.token_address = l.token_address
   AND r.ts_hour = l.ts_hour
  JOIN ema_agg ea
    ON ea.token_address = l.token_address
   AND ea.ts_hour = l.ts_hour
  LEFT JOIN supplies s
    ON s.token_address = l.token_address
)

SELECT *
FROM feats
ORDER BY token_address, ts_hour
