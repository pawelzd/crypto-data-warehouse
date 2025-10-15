WITH base AS (
  SELECT
    c.address AS token_address,
    SAFE_CAST(c.volume AS FLOAT64) AS volume,
    c.datetime AS ts_hour,
    AVG(SAFE_CAST(c.price AS FLOAT64)) AS price,
    SAFE_CAST(t.circSupply AS FLOAT64) AS total_supply
  FROM {{ ref('20m_cv_prod_filled_hours') }} c
  LEFT JOIN {{ source('core', 'token_metadata_jup_tmp') }} t
    ON c.address = t.id
  where t.mcap >= 20000000
  GROUP BY
    c.address, 
    ts_hour, t.circSupply, c.volume
),

lags AS (
  SELECT
    token_address,
    ts_hour,
    price,
    volume,
    total_supply,
    ROW_NUMBER() OVER (PARTITION BY token_address ORDER BY ts_hour) AS rn,
    LAG(volume, 1)  OVER (PARTITION BY token_address ORDER BY ts_hour) AS volume_lag_1h,
    LAG(volume, 24) OVER (PARTITION BY token_address ORDER BY ts_hour) AS volume_lag_24h,
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
    ts_hour,
    price,
    price_lag1,
    SAFE_DIVIDE(price, price_lag1) - 1 AS ret_1h,
    CASE
      WHEN price > 0 AND price_lag1 > 0 THEN LOG(price) - LOG(price_lag1)
      ELSE NULL
    END AS logret_1h,
    rn,
    volume,
    total_supply,
    SUM(volume) OVER w_6h   AS volume_sum_6h,
    SUM(volume) OVER w_24h  AS volume_sum_24h,
    SUM(volume) OVER w_168h AS volume_sum_168h,
    AVG(volume) OVER w_24h  AS volume_mean_24h,
    AVG(volume) OVER w_168h AS volume_mean_168h,
    STDDEV_SAMP(volume) OVER w_24h  AS volume_std_24h,
    STDDEV_SAMP(volume) OVER w_168h AS volume_std_168h,
    COUNT(*) OVER w_24h AS volume_n_24h
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

rets_with_lag AS (
  SELECT
    r.*,
    0.75 AS alpha_fast,
    0.90 AS alpha_slow,
    POW(0.75, rn) AS a_fast_pow,
    POW(0.90, rn) AS a_slow_pow,
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
    volume,
    total_supply,
    volume_sum_6h,
    volume_sum_24h,
    volume_sum_168h,
    volume_mean_24h,
    volume_mean_168h,
    volume_std_24h,
    volume_std_168h,
    volume_n_24h,
    a_fast_pow,
    a_slow_pow,
    ret_1h_lag1,


    LEAST(GREATEST(SAFE_DIVIDE(volume - LAG(volume,1)  OVER (PARTITION BY token_address ORDER BY ts_hour),
                               NULLIF(LAG(volume,1) OVER (PARTITION BY token_address ORDER BY ts_hour),0)), -10), 10) AS volume_ret_1h,
    LEAST(GREATEST(SAFE_DIVIDE(volume - LAG(volume,24) OVER (PARTITION BY token_address ORDER BY ts_hour),
                               NULLIF(LAG(volume,24) OVER (PARTITION BY token_address ORDER BY ts_hour),0)), -10),10) AS volume_ret_24h,

    LOG(1 + volume) AS log_volume,
    LOG(1 + SAFE_DIVIDE(volume, NULLIF(total_supply,0))) AS log_volume_per_supply,

    LOG(1 + volume_mean_24h)                 AS log_volume_mean_24h,
    LOG(1 + volume_mean_168h)                AS log_volume_mean_168h,
    LOG(1 + SAFE_DIVIDE(volume_mean_24h,  NULLIF(total_supply,0))) AS log_volume_mean_24h_per_supply,
    LOG(1 + SAFE_DIVIDE(volume_mean_168h, NULLIF(total_supply,0))) AS log_volume_mean_168h_per_supply,

    SAFE_DIVIDE(volume_std_24h,  NULLIF(volume_mean_24h,  0)) AS volume_cv_24h,
    SAFE_DIVIDE(volume_std_168h, NULLIF(volume_mean_168h, 0)) AS volume_cv_168h,

    SAFE_DIVIDE(volume, NULLIF(SAFE_DIVIDE(volume_sum_24h - volume, NULLIF(volume_n_24h - 1, 0)), 0)) AS volume_spike_ratio_24h_excl,
    SAFE_DIVIDE(volume - volume_mean_24h, NULLIF(volume_std_24h, 0)) AS volume_z_24h,

    SAFE_DIVIDE(volume_sum_6h  - volume_sum_24h,  NULLIF(volume_sum_24h,  0)) AS volume_accel_6v24,
    SAFE_DIVIDE(volume_sum_24h - volume_sum_168h, NULLIF(volume_sum_168h, 0)) AS volume_accel_24v168,

    -- expose EMAs with the exact names you want (keep them, you can comment in downstream)
    
    SAFE_DIVIDE(SUM(volume * a_fast_pow) OVER w_unbounded, NULLIF(SUM(a_fast_pow) OVER w_unbounded, 0)) AS volume_ema_fast,
    SAFE_DIVIDE(SUM(volume * a_slow_pow) OVER w_unbounded, NULLIF(SUM(a_slow_pow) OVER w_unbounded, 0)) AS volume_ema_slow,

    -- ADDED: 12h stats for ret_over_rv_12h + general usefulness
    AVG(ret_1h) OVER w12  AS mean_ret_12h,
    STDDEV_SAMP(ret_1h) OVER w12 AS std_ret_12h,

    AVG(ret_1h) OVER w24  AS mean_ret_24h,
    STDDEV_SAMP(ret_1h) OVER w24 AS std_ret_24h,
    AVG(ret_1h) OVER w72  AS mean_ret_72h,
    STDDEV_SAMP(ret_1h) OVER w72 AS std_ret_72h,
    AVG(ret_1h) OVER w168 AS mean_ret_168h,
    STDDEV_SAMP(ret_1h) OVER w168 AS std_ret_168h,

    -- Realized vol across horizons (used for ratios)
    SQRT(SUM(POW(COALESCE(logret_1h,0), 2)) OVER w4)   * SQRT(24) AS rv_4h,
    SQRT(SUM(POW(COALESCE(logret_1h,0), 2)) OVER w12)  * SQRT(24) AS rv_12h,
    SQRT(SUM(POW(COALESCE(logret_1h,0), 2)) OVER w24)  * SQRT(24) AS rv_24h,
    SQRT(SUM(POW(COALESCE(logret_1h,0), 2)) OVER w48)  * SQRT(24) AS rv_48h,
    SQRT(SUM(POW(COALESCE(logret_1h,0), 2)) OVER w72)  * SQRT(24) AS rv_72h,
    SQRT(SUM(POW(COALESCE(logret_1h,0), 2)) OVER w168) * SQRT(24) AS rv_7d,

    SAFE_DIVIDE(AVG(ret_1h) OVER w24,  NULLIF(STDDEV_SAMP(ret_1h) OVER w24, 0)) * SQRT(24) AS sharpe_24h,
    SAFE_DIVIDE(AVG(ret_1h) OVER w48,  NULLIF(STDDEV_SAMP(ret_1h) OVER w48, 0)) * SQRT(24) AS sharpe_48h,
    SAFE_DIVIDE(AVG(ret_1h) OVER w168, NULLIF(STDDEV_SAMP(ret_1h) OVER w168,0)) * SQRT(24) AS sharpe_7d,

    SAFE_DIVIDE(ret_1h - AVG(ret_1h) OVER w24, NULLIF(STDDEV_SAMP(ret_1h) OVER w24,0)) AS ret_z_24h,

    EXP(SUM(COALESCE(logret_1h,0)) OVER w24)  - 1 AS cumret_24h,
    EXP(SUM(COALESCE(logret_1h,0)) OVER w168) - 1 AS cumret_7d,

    -- MAs (add 48h for contrasts)
    AVG(price) OVER w6   AS sma_6h,
    AVG(price) OVER w12  AS sma_12h,
    AVG(price) OVER w24  AS sma_24h,
    AVG(price) OVER w48  AS sma_48h,
    AVG(price) OVER w72  AS sma_72h,
    AVG(price) OVER w168 AS sma_168h,

    (AVG(price) OVER w12) - (AVG(price) OVER w26) AS macd_sma_12_26h,

    (price - AVG(price) OVER w24) / NULLIF(STDDEV_SAMP(price) OVER w24,0) AS price_z_24h,

    SAFE_DIVIDE(
      price - MIN(price) OVER w24,
      NULLIF(MAX(price) OVER w24 - MIN(price) OVER w24, 0)
    ) AS pct_in_range_24h,

    -- Local extremes
    price / NULLIF(MAX(price) OVER w4, 0)  - 1 AS dist_to_high_4h,
    price / NULLIF(MIN(price) OVER w4, 0)  - 1 AS dist_to_low_4h,
    price / NULLIF(MAX(price) OVER w12, 0) - 1 AS dist_to_high_12h,
    price / NULLIF(MIN(price) OVER w12, 0) - 1 AS dist_to_low_12h,
    price / NULLIF(MAX(price) OVER w24, 0) - 1 AS dist_to_high_24h,
    price / NULLIF(MIN(price) OVER w24, 0) - 1 AS dist_to_low_24h,

    CASE WHEN price > MAX(price) OVER (PARTITION BY token_address ORDER BY ts_hour
                                       ROWS BETWEEN 24 PRECEDING AND 1 PRECEDING)
          THEN 1 ELSE 0 END AS breakout_high_24h,
    CASE WHEN price < MIN(price) OVER (PARTITION BY token_address ORDER BY ts_hour
                                       ROWS BETWEEN 24 PRECEDING AND 1 PRECEDING)
          THEN 1 ELSE 0 END AS breakout_low_24h,

    price / NULLIF(MAX(price) OVER w168, 0) - 1 AS drawdown_7d,
    price / NULLIF(MAX(price) OVER w24, 0)  - 1 AS drawdown_24h,
    price / NULLIF(MAX(price) OVER w48, 0)  - 1 AS drawdown_48h,

    CORR(ret_1h, ret_1h_lag1) OVER w72 AS acf1_72h,

    CASE WHEN COUNTIF(ret_1h IS NOT NULL) OVER w24  = 24  THEN 1 ELSE 0 END AS has_24h,
    CASE WHEN COUNTIF(ret_1h IS NOT NULL) OVER w72  = 72  THEN 1 ELSE 0 END AS has_72h,
    CASE WHEN COUNTIF(ret_1h IS NOT NULL) OVER w168 = 168 THEN 1 ELSE 0 END AS has_168h
  FROM rets_with_lag
  WINDOW
    w_unbounded AS (PARTITION BY token_address ORDER BY UNIX_SECONDS(ts_hour) ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW),
    w4   AS (PARTITION BY token_address ORDER BY ts_hour ROWS BETWEEN 3   PRECEDING AND CURRENT ROW),
    w6   AS (PARTITION BY token_address ORDER BY ts_hour ROWS BETWEEN 5   PRECEDING AND CURRENT ROW),
    w12  AS (PARTITION BY token_address ORDER BY ts_hour ROWS BETWEEN 11  PRECEDING AND CURRENT ROW),
    w24  AS (PARTITION BY token_address ORDER BY ts_hour ROWS BETWEEN 23  PRECEDING AND CURRENT ROW),
    w26  AS (PARTITION BY token_address ORDER BY ts_hour ROWS BETWEEN 25  PRECEDING AND CURRENT ROW),
    w48  AS (PARTITION BY token_address ORDER BY ts_hour ROWS BETWEEN 47  PRECEDING AND CURRENT ROW),
    w72  AS (PARTITION BY token_address ORDER BY ts_hour ROWS BETWEEN 71  PRECEDING AND CURRENT ROW),
    w168 AS (PARTITION BY token_address ORDER BY ts_hour ROWS BETWEEN 167 PRECEDING AND CURRENT ROW)
),

rsi AS (
  SELECT
    token_address,
    ts_hour,
    price,
    ret_1h, logret_1h,
    mean_ret_12h, std_ret_12h,
    mean_ret_24h, std_ret_24h, mean_ret_72h, std_ret_72h, mean_ret_168h, std_ret_168h,
    rv_4h, rv_12h, rv_24h, rv_7d,
    sharpe_24h, sharpe_48h, sharpe_7d,
    ret_z_24h, cumret_24h, cumret_7d,
    sma_6h, sma_12h, sma_24h, sma_48h, sma_72h, sma_168h,
    macd_sma_12_26h,
    price_z_24h, pct_in_range_24h,
    dist_to_high_4h, dist_to_low_4h,
    dist_to_high_12h, dist_to_low_12h,
    dist_to_high_24h, dist_to_low_24h,
    breakout_high_24h, breakout_low_24h,
    drawdown_7d, drawdown_24h, drawdown_48h,
    acf1_72h,
    has_24h, has_72h, has_168h,
    volume_ret_1h,
    volume_ret_24h,
    log_volume,
    log_volume_per_supply,
    log_volume_mean_24h,
    log_volume_mean_168h,
    log_volume_mean_24h_per_supply,
    log_volume_mean_168h_per_supply,
    volume_cv_24h,
    volume_cv_168h,
    volume_spike_ratio_24h_excl,
    volume_z_24h,
    volume_accel_6v24,
    volume_accel_24v168,
    volume, total_supply,
    volume_sum_6h, volume_sum_24h, volume_sum_168h,
    volume_mean_24h, volume_mean_168h,
    volume_std_24h, volume_std_168h,
    volume_n_24h,
    volume_ema_fast, volume_ema_slow,

    GREATEST(ret_1h, 0)  AS gain,
    GREATEST(-ret_1h, 0) AS loss,

    AVG(GREATEST(ret_1h, 0))  OVER (PARTITION BY token_address ORDER BY ts_hour ROWS BETWEEN 13 PRECEDING AND CURRENT ROW) AS avg_gain_14,
    AVG(GREATEST(-ret_1h, 0)) OVER (PARTITION BY token_address ORDER BY ts_hour ROWS BETWEEN 13 PRECEDING AND CURRENT ROW) AS avg_loss_14
  FROM roll
),

final AS (
  SELECT
    r.token_address,
    r.ts_hour,
    r.price,

    -- returns
    COALESCE(r.ret_1h, 0)     AS ret_1h,
    COALESCE(r.logret_1h, 0)  AS logret_1h,

    -- realized volatility & return stats
    COALESCE(r.mean_ret_24h, 0)   AS mean_ret_24h,
    COALESCE(r.std_ret_24h, 0)    AS std_ret_24h,
    COALESCE(r.mean_ret_72h, 0)   AS mean_ret_72h,
    COALESCE(r.std_ret_72h, 0)    AS std_ret_72h,
    COALESCE(r.mean_ret_168h, 0)  AS mean_ret_168h,
    COALESCE(r.std_ret_168h, 0)   AS std_ret_168h,
    COALESCE(r.rv_24h, 0)         AS rv_24h,
    COALESCE(r.rv_4h, 0)          AS rv_4h,
    COALESCE(r.rv_12h, 0)         AS rv_12h,
    COALESCE(r.rv_7d, 0)          AS rv_7d,
    COALESCE(r.sharpe_24h, 0)     AS sharpe_24h,
    COALESCE(r.sharpe_7d, 0)      AS sharpe_7d,
    COALESCE(r.ret_z_24h, 0)      AS ret_z_24h,
    COALESCE(r.cumret_24h, 0)     AS cumret_24h,
    COALESCE(r.cumret_7d, 0)      AS cumret_7d,
    COALESCE(r.volume_ret_1h, 0)  AS volume_ret_1h,
    COALESCE(r.volume_ret_24h, 0) AS volume_ret_24h,
    COALESCE(r.log_volume, 0) AS log_volume,
    COALESCE(r.log_volume_per_supply, 0) AS log_volume_per_supply,
    COALESCE(r.log_volume_mean_24h, 0) AS log_volume_mean_24h,
    COALESCE(r.log_volume_mean_168h, 0) AS log_volume_mean_168h,
    COALESCE(r.log_volume_mean_24h_per_supply, 0) AS log_volume_mean_24h_per_supply,
    COALESCE(r.log_volume_mean_168h_per_supply, 0) AS log_volume_mean_168h_per_supply,
    COALESCE(r.volume_cv_24h, 0) AS volume_cv_24h,
    COALESCE(r.volume_cv_168h, 0) AS volume_cv_168h,
    COALESCE(r.volume_spike_ratio_24h_excl, 0) AS volume_spike_ratio_24h_excl,
    COALESCE(r.volume_z_24h, 0) AS volume_z_24h,
    COALESCE(r.volume_accel_6v24, 0) AS volume_accel_6v24,
    COALESCE(r.volume_accel_24v168, 0) AS volume_accel_24v168,
    --COALESCE(r.volume, 0) AS volume,
    --COALESCE(r.total_supply, 0) AS total_supply,
    COALESCE(r.volume_sum_6h, 0) AS volume_sum_6h,
    COALESCE(r.volume_sum_24h, 0) AS volume_sum_24h,
    COALESCE(r.volume_sum_168h, 0) AS volume_sum_168h,
    COALESCE(r.volume_mean_24h, 0) AS volume_mean_24h,
    COALESCE(r.volume_mean_168h, 0) AS volume_mean_168h,
    COALESCE(r.volume_std_24h, 0) AS volume_std_24h,
    COALESCE(r.volume_std_168h, 0) AS volume_std_168h,
    COALESCE(r.volume_n_24h, 0) AS volume_n_24h,
    COALESCE(r.volume_ema_fast, 0) AS volume_ema_fast,
    COALESCE(r.volume_ema_slow, 0) AS volume_ema_slow,
    -- price MAs and distances
    COALESCE(r.sma_6h, 0)         AS sma_6h,
    COALESCE(r.sma_12h, 0)        AS sma_12h,
    COALESCE(r.sma_24h, 0)        AS sma_24h,
    COALESCE(r.sma_48h, 0)        AS sma_48h,
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

    COALESCE(r.dist_to_high_24h, 0) AS dist_to_high_24h,
    COALESCE(r.dist_to_low_24h, 0)  AS dist_to_low_24h,
    COALESCE(r.dist_to_high_4h, 0)  AS dist_to_high_4h,
    COALESCE(r.dist_to_low_4h, 0)   AS dist_to_low_4h,
    COALESCE(r.dist_to_high_12h, 0) AS dist_to_high_12h,
    COALESCE(r.dist_to_low_12h, 0)  AS dist_to_low_12h,
    COALESCE(r.breakout_high_24h, 0) AS breakout_high_24h,
    COALESCE(r.breakout_low_24h, 0)  AS breakout_low_24h,
    COALESCE(r.drawdown_7d, 0)      AS drawdown_7d,
    COALESCE(r.drawdown_48h, 0)     AS drawdown_48h,
    COALESCE(r.drawdown_24h, 0)     AS drawdown_24h,
    r.has_168h,

    -- RSI
    CASE
      WHEN r.avg_loss_14 IS NULL OR r.avg_loss_14 = 0 THEN 100
      ELSE 100 - 100 / (1 + COALESCE(SAFE_DIVIDE(r.avg_gain_14, NULLIF(r.avg_loss_14, 0)), 0))
    END AS rsi_14,

    COALESCE(r.acf1_72h, 0) AS acf1_72h,

    EXTRACT(DAYOFWEEK FROM r.ts_hour) AS dow_1_sun_7_sat,
    EXTRACT(HOUR FROM r.ts_hour)      AS hour_of_day,
    SIN(2 * 3.141592653589793 * EXTRACT(HOUR FROM r.ts_hour) / 24.0) AS sin_hour,
    COS(2 * 3.141592653589793 * EXTRACT(HOUR FROM r.ts_hour) / 24.0) AS cos_hour,
    SIN(2 * 3.141592653589793 * CAST(EXTRACT(DAYOFWEEK FROM r.ts_hour) AS FLOAT64) / 7.0) AS sin_dow,
    COS(2 * 3.141592653589793 * CAST(EXTRACT(DAYOFWEEK FROM r.ts_hour) AS FLOAT64) / 7.0) AS cos_dow,

    -- relative vols (volatility)
    COALESCE(SAFE_DIVIDE(r.rv_24h, NULLIF(r.rv_7d, 0)), 0) AS vol_ratio_24_7d,
    COALESCE(SAFE_DIVIDE(r.std_ret_24h, r.std_ret_72h), 0) AS vol_ratio_24_72,
    COALESCE(SAFE_DIVIDE(r.std_ret_72h, r.std_ret_168h), 0) AS vol_ratio_72_168,
    COALESCE(SAFE_DIVIDE(r.rv_4h,  NULLIF(r.rv_24h, 0)), 0) AS vol_ratio_4_24,
    COALESCE(SAFE_DIVIDE(r.rv_12h, NULLIF(r.rv_24h, 0)), 0) AS vol_ratio_12_24,
    COALESCE(SAFE_DIVIDE(r.mean_ret_12h, NULLIF(r.rv_12h, 0)), 0) AS ret_over_rv_12h

    -- NEW: expose supply & volume features
    -- r.volume,
  FROM rsi r
),

distances_and_slopes AS (
  SELECT
    f.*,

    -- NEW contrast (relative to two MAs)
    COALESCE(f.dist_to_sma_12h - (SAFE_DIVIDE(f.price, NULLIF(f.sma_48h,0)) - 1), 0) AS sma_diff_12_48,

    -- keep your existing fast/slow diff
    COALESCE(f.dist_to_sma_6h - f.dist_to_sma_24h, 0) AS sma_diff_fast_slow,

    -- NEW slope variants (per-hour change over N hours)
    COALESCE( (f.dist_to_sma_6h  - LAG(f.dist_to_sma_6h,  12) OVER w) / 12, 0) AS sma6h_slope_12h,
    COALESCE( (f.dist_to_sma_6h  - LAG(f.dist_to_sma_6h,  24) OVER w) / 24, 0) AS sma6h_slope_24h,
    COALESCE( (f.dist_to_sma_12h - LAG(f.dist_to_sma_12h, 12) OVER w) / 12, 0) AS sma12h_slope_12h,
    COALESCE( (f.dist_to_sma_12h - LAG(f.dist_to_sma_12h, 24) OVER w) / 24, 0) AS sma12h_slope_24h,
    COALESCE( (f.dist_to_sma_12h - LAG(f.dist_to_sma_12h, 72) OVER w) / 72, 0) AS sma12h_slope_72h,
    COALESCE( (f.dist_to_sma_24h - LAG(f.dist_to_sma_24h, 24) OVER w) / 24, 0) AS sma24h_slope_24h,
    COALESCE( (SAFE_DIVIDE(f.price, NULLIF(f.sma_48h,0)) - 1
               - LAG(SAFE_DIVIDE(f.price, NULLIF(f.sma_48h,0)) - 1, 24) OVER w) / 24, 0) AS sma48h_slope_24h,
    (sharpe_24h - sharpe_7d) AS sharpe_delta,
    -- Interaction kept (relative by construction)
    COALESCE(SAFE_DIVIDE(f.rsi_14 * f.vol_ratio_24_7d, 100), 0) AS rsi_vol_interaction

  FROM final f
  WINDOW w AS (PARTITION BY f.token_address ORDER BY f.ts_hour)
)

SELECT *
FROM distances_and_slopes

