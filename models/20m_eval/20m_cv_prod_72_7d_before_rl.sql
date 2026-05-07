{{ config(
    materialized = 'view'
) }}

-- ============================================================
-- Timeframe framework: 4h · 12h · 24h · 72h · 168h
--
-- 4h   — pump detection (most memecoin moves initiate within 2–6h;
--         4h is the primary reference candle used by crypto traders)
-- 12h  — half-day momentum confirmation (sustained vs fading)
-- 24h  — daily cycle (most important; volume/momentum reference)
-- 72h  — 3-day medium trend (aligns with typical memecoin trend exhaustion)
-- 168h — weekly baseline / regime context (drawdown, SMA denominator)
--
-- Removed: 6h (replaced by 4h), 48h (redundant between 24h and 72h)
-- Volume windows: 4h / 24h / 72h  (dropped 168h — unreliable for tokens
-- that may have been 10× smaller or non-existent 7 days ago)
-- ============================================================

with meta AS (
  SELECT address,
    MAX(circulating_supply) AS circulating_supply,
    MAX(market_cap)         AS market_cap,
    MAX(update_date)           AS datetime
  FROM {{ source('streamed_datapublic', 'public_tokens_to_monitor') }}
  GROUP BY address
),

base AS (
  SELECT
    c.address                        AS token_address,
    SAFE_CAST(c.volume AS FLOAT64)   AS volume,
    c.datetime                       AS ts_hour,
    AVG(SAFE_CAST(c.price AS FLOAT64)) AS price,
    SAFE_CAST(t.circulating_supply AS FLOAT64) AS total_supply
  FROM {{ ref('20m_cv_prod_filled_hours') }} c
  LEFT JOIN meta t
    ON  c.address = t.address
    AND DATE(c.datetime) = DATE(t.datetime)
  WHERE t.market_cap >= 20000000
  GROUP BY c.address, ts_hour, t.circulating_supply, c.volume
),

lags AS (
  SELECT
    token_address,
    ts_hour,
    price,
    volume,
    total_supply,
    ROW_NUMBER() OVER (PARTITION BY token_address ORDER BY ts_hour) AS rn,
    LAG(volume,  1) OVER (PARTITION BY token_address ORDER BY ts_hour) AS volume_lag_1h,
    LAG(volume, 24) OVER (PARTITION BY token_address ORDER BY ts_hour) AS volume_lag_24h,
    LAG(price,  1)  OVER (PARTITION BY token_address ORDER BY ts_hour) AS price_lag1,
    LAG(price,  4)  OVER (PARTITION BY token_address ORDER BY ts_hour) AS price_lag4,
    LAG(price, 12)  OVER (PARTITION BY token_address ORDER BY ts_hour) AS price_lag12,
    LAG(price, 24)  OVER (PARTITION BY token_address ORDER BY ts_hour) AS price_lag24,
    LAG(price, 72)  OVER (PARTITION BY token_address ORDER BY ts_hour) AS price_lag72,
    LAG(price, 168) OVER (PARTITION BY token_address ORDER BY ts_hour) AS price_lag168
  FROM base
),

rets AS (
  -- Volume windows use RANGE (seconds) to be robust to missing hours.
  -- 4h=14400s · 24h=86400s · 72h=259200s
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
    SUM(volume) OVER w_4h  AS volume_sum_4h,
    SUM(volume) OVER w_24h AS volume_sum_24h,
    SUM(volume) OVER w_72h AS volume_sum_72h,
    AVG(volume) OVER w_24h AS volume_mean_24h,
    AVG(volume) OVER w_72h AS volume_mean_72h,
    STDDEV_SAMP(volume) OVER w_24h AS volume_std_24h,
    STDDEV_SAMP(volume) OVER w_72h AS volume_std_72h,
    COUNT(*)    OVER w_24h AS volume_n_24h
  FROM lags
  WINDOW
    w_4h  AS (PARTITION BY token_address ORDER BY UNIX_SECONDS(ts_hour) RANGE BETWEEN  14400 PRECEDING AND CURRENT ROW),
    w_24h AS (PARTITION BY token_address ORDER BY UNIX_SECONDS(ts_hour) RANGE BETWEEN  86400 PRECEDING AND CURRENT ROW),
    w_72h AS (PARTITION BY token_address ORDER BY UNIX_SECONDS(ts_hour) RANGE BETWEEN 259200 PRECEDING AND CURRENT ROW)
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
    rn,
    volume,
    total_supply,
    volume_sum_4h,
    volume_sum_24h,
    volume_sum_72h,
    volume_mean_24h,
    volume_mean_72h,
    volume_std_24h,
    volume_std_72h,
    volume_n_24h,
    a_fast_pow,
    a_slow_pow,
    ret_1h_lag1,

    -- ── Volume change rates ──────────────────────────────────────────────────
    LEAST(GREATEST(SAFE_DIVIDE(
      volume - LAG(volume, 1) OVER (PARTITION BY token_address ORDER BY ts_hour),
      NULLIF(LAG(volume, 1) OVER (PARTITION BY token_address ORDER BY ts_hour), 0)
    ), -10), 10) AS volume_ret_1h,

    LEAST(GREATEST(SAFE_DIVIDE(
      volume - LAG(volume, 24) OVER (PARTITION BY token_address ORDER BY ts_hour),
      NULLIF(LAG(volume, 24) OVER (PARTITION BY token_address ORDER BY ts_hour), 0)
    ), -10), 10) AS volume_ret_24h,

    -- ── Volume level features ────────────────────────────────────────────────
    LOG(1 + volume)                                                          AS log_volume,
    LOG(1 + SAFE_DIVIDE(volume, NULLIF(total_supply, 0)))                    AS log_volume_per_supply,
    LOG(1 + volume_mean_24h)                                                 AS log_volume_mean_24h,
    LOG(1 + volume_mean_72h)                                                 AS log_volume_mean_72h,
    LOG(1 + SAFE_DIVIDE(volume_mean_24h, NULLIF(total_supply, 0)))           AS log_volume_mean_24h_per_supply,
    LOG(1 + SAFE_DIVIDE(volume_mean_72h, NULLIF(total_supply, 0)))           AS log_volume_mean_72h_per_supply,

    -- ── Volume dispersion & spike ────────────────────────────────────────────
    SAFE_DIVIDE(volume_std_24h, NULLIF(volume_mean_24h, 0))                  AS volume_cv_24h,
    SAFE_DIVIDE(volume_std_72h, NULLIF(volume_mean_72h, 0))                  AS volume_cv_72h,
    -- spike: current vs trailing mean excluding self
    SAFE_DIVIDE(volume,
      NULLIF(SAFE_DIVIDE(volume_sum_24h - volume,
                         NULLIF(volume_n_24h - 1, 0)), 0))                   AS volume_spike_ratio_24h_excl,
    SAFE_DIVIDE(volume - volume_mean_24h, NULLIF(volume_std_24h, 0))         AS volume_z_24h,

    -- ── Volume acceleration ──────────────────────────────────────────────────
    -- Interpretation: >0 means recent window concentrates more volume than uniform
    -- 4v24: last 4h share of 24h volume vs uniform (neutral = -0.83)
    SAFE_DIVIDE(volume_sum_4h  - volume_sum_24h,  NULLIF(volume_sum_24h,  0)) AS volume_accel_4v24,
    -- 24v72: last 24h share of 72h volume vs uniform (neutral = -0.67)
    SAFE_DIVIDE(volume_sum_24h - volume_sum_72h,  NULLIF(volume_sum_72h,  0)) AS volume_accel_24v72,

    -- ── Exponential volume MAs (unbounded decay from series start) ───────────
    SAFE_DIVIDE(SUM(volume * a_fast_pow) OVER w_unbounded,
                NULLIF(SUM(a_fast_pow) OVER w_unbounded, 0))                 AS volume_ema_fast,
    SAFE_DIVIDE(SUM(volume * a_slow_pow) OVER w_unbounded,
                NULLIF(SUM(a_slow_pow) OVER w_unbounded, 0))                 AS volume_ema_slow,

    -- ── VWAP (volume-weighted average price) ─────────────────────────────────
    -- dist_to_vwap_Xh = price above/below flow-weighted fair value
    SAFE_DIVIDE(SUM(price * COALESCE(volume, 0)) OVER w4,
                NULLIF(SUM(COALESCE(volume, 0)) OVER w4,  0))               AS vwap_4h,
    SAFE_DIVIDE(SUM(price * COALESCE(volume, 0)) OVER w24,
                NULLIF(SUM(COALESCE(volume, 0)) OVER w24, 0))               AS vwap_24h,

    -- ── Amihud illiquidity ────────────────────────────────────────────────────
    -- |ret| / volume: how much does price move per unit of volume traded?
    -- High = thin book = large slippage at $1k. Key complement to slippage_bps_proxy.
    SAFE_DIVIDE(ABS(COALESCE(ret_1h, 0)), NULLIF(volume, 0))                AS amihud_1h,
    AVG(SAFE_DIVIDE(ABS(COALESCE(ret_1h, 0)), NULLIF(volume, 0))) OVER w24  AS amihud_mean_24h,
    AVG(SAFE_DIVIDE(ABS(COALESCE(ret_1h, 0)), NULLIF(volume, 0))) OVER w72  AS amihud_mean_72h,

    -- ── Return statistics across timeframes ──────────────────────────────────
    AVG(ret_1h) OVER w4   AS mean_ret_4h,    -- very short momentum
    STDDEV_SAMP(ret_1h) OVER w4  AS std_ret_4h,
    AVG(ret_1h) OVER w12  AS mean_ret_12h,
    STDDEV_SAMP(ret_1h) OVER w12 AS std_ret_12h,
    AVG(ret_1h) OVER w24  AS mean_ret_24h,
    STDDEV_SAMP(ret_1h) OVER w24 AS std_ret_24h,
    AVG(ret_1h) OVER w72  AS mean_ret_72h,
    STDDEV_SAMP(ret_1h) OVER w72 AS std_ret_72h,
    AVG(ret_1h) OVER w168 AS mean_ret_168h,
    STDDEV_SAMP(ret_1h) OVER w168 AS std_ret_168h,

    -- ── Realized volatility (annualized to daily) ─────────────────────────────
    SQRT(SUM(POW(COALESCE(logret_1h, 0), 2)) OVER w4)   * SQRT(24) AS rv_4h,
    SQRT(SUM(POW(COALESCE(logret_1h, 0), 2)) OVER w12)  * SQRT(24) AS rv_12h,
    SQRT(SUM(POW(COALESCE(logret_1h, 0), 2)) OVER w24)  * SQRT(24) AS rv_24h,
    SQRT(SUM(POW(COALESCE(logret_1h, 0), 2)) OVER w72)  * SQRT(24) AS rv_72h,
    SQRT(SUM(POW(COALESCE(logret_1h, 0), 2)) OVER w168) * SQRT(24) AS rv_7d,

    -- ── Sharpe ratio across timeframes ───────────────────────────────────────
    -- 4h Sharpe: quality of immediate momentum (reward/risk over last 4h)
    SAFE_DIVIDE(AVG(ret_1h) OVER w4,   NULLIF(STDDEV_SAMP(ret_1h) OVER w4,   0)) * SQRT(24) AS sharpe_4h,
    SAFE_DIVIDE(AVG(ret_1h) OVER w24,  NULLIF(STDDEV_SAMP(ret_1h) OVER w24,  0)) * SQRT(24) AS sharpe_24h,
    SAFE_DIVIDE(AVG(ret_1h) OVER w72,  NULLIF(STDDEV_SAMP(ret_1h) OVER w72,  0)) * SQRT(24) AS sharpe_72h,
    SAFE_DIVIDE(AVG(ret_1h) OVER w168, NULLIF(STDDEV_SAMP(ret_1h) OVER w168, 0)) * SQRT(24) AS sharpe_7d,

    -- ── Standardized current return ───────────────────────────────────────────
    SAFE_DIVIDE(ret_1h - AVG(ret_1h) OVER w24,
                NULLIF(STDDEV_SAMP(ret_1h) OVER w24, 0))                        AS ret_z_24h,
    SAFE_DIVIDE(ret_1h - AVG(ret_1h) OVER w72,
                NULLIF(STDDEV_SAMP(ret_1h) OVER w72, 0))                        AS ret_z_72h,

    -- ── Skewness inputs (method of moments, 24h) ──────────────────────────────
    AVG(POW(COALESCE(ret_1h, 0), 2)) OVER w24 AS ret_sq_mean_24h,
    AVG(POW(COALESCE(ret_1h, 0), 3)) OVER w24 AS ret_cu_mean_24h,

    -- ── Cumulative returns ────────────────────────────────────────────────────
    -- Short horizons critical for entry timing: catching early vs late in a move
    EXP(SUM(COALESCE(logret_1h, 0)) OVER w4)   - 1 AS cumret_4h,
    EXP(SUM(COALESCE(logret_1h, 0)) OVER w12)  - 1 AS cumret_12h,
    EXP(SUM(COALESCE(logret_1h, 0)) OVER w24)  - 1 AS cumret_24h,
    EXP(SUM(COALESCE(logret_1h, 0)) OVER w168) - 1 AS cumret_7d,

    -- ── Price moving averages ─────────────────────────────────────────────────
    -- 4h SMA: replaces 6h (4h is primary crypto trader reference timeframe)
    -- Removed 48h (redundant between 24h and 72h)
    AVG(price) OVER w4   AS sma_4h,
    AVG(price) OVER w12  AS sma_12h,
    AVG(price) OVER w24  AS sma_24h,
    AVG(price) OVER w72  AS sma_72h,
    AVG(price) OVER w168 AS sma_168h,
    -- MACD: 12h minus 26h SMA (standard crypto momentum signal)
    (AVG(price) OVER w12) - (AVG(price) OVER w26) AS macd_sma_12_26h,

    -- ── Price normalization ───────────────────────────────────────────────────
    (price - AVG(price) OVER w24) / NULLIF(STDDEV_SAMP(price) OVER w24, 0)  AS price_z_24h,
    (price - AVG(price) OVER w72) / NULLIF(STDDEV_SAMP(price) OVER w72, 0)  AS price_z_72h,

    SAFE_DIVIDE(price - MIN(price) OVER w24,
                NULLIF(MAX(price) OVER w24 - MIN(price) OVER w24, 0))        AS pct_in_range_24h,
    SAFE_DIVIDE(price - MIN(price) OVER w72,
                NULLIF(MAX(price) OVER w72 - MIN(price) OVER w72, 0))        AS pct_in_range_72h,

    -- ── Distance from local extremes ─────────────────────────────────────────
    price / NULLIF(MAX(price) OVER w4,   0) - 1 AS dist_to_high_4h,
    price / NULLIF(MIN(price) OVER w4,   0) - 1 AS dist_to_low_4h,
    price / NULLIF(MAX(price) OVER w12,  0) - 1 AS dist_to_high_12h,
    price / NULLIF(MIN(price) OVER w12,  0) - 1 AS dist_to_low_12h,
    price / NULLIF(MAX(price) OVER w24,  0) - 1 AS dist_to_high_24h,
    price / NULLIF(MIN(price) OVER w24,  0) - 1 AS dist_to_low_24h,
    price / NULLIF(MAX(price) OVER w72,  0) - 1 AS dist_to_high_72h,
    price / NULLIF(MIN(price) OVER w72,  0) - 1 AS dist_to_low_72h,

    -- ── Breakout flags ────────────────────────────────────────────────────────
    -- Multi-horizon: fresh breakout (4h) vs confirmed (24h) vs major (72h)
    CASE WHEN price > MAX(price) OVER (PARTITION BY token_address ORDER BY ts_hour
                                       ROWS BETWEEN 4   PRECEDING AND 1 PRECEDING)
          THEN 1 ELSE 0 END AS breakout_high_4h,
    CASE WHEN price < MIN(price) OVER (PARTITION BY token_address ORDER BY ts_hour
                                       ROWS BETWEEN 4   PRECEDING AND 1 PRECEDING)
          THEN 1 ELSE 0 END AS breakout_low_4h,
    CASE WHEN price > MAX(price) OVER (PARTITION BY token_address ORDER BY ts_hour
                                       ROWS BETWEEN 24  PRECEDING AND 1 PRECEDING)
          THEN 1 ELSE 0 END AS breakout_high_24h,
    CASE WHEN price < MIN(price) OVER (PARTITION BY token_address ORDER BY ts_hour
                                       ROWS BETWEEN 24  PRECEDING AND 1 PRECEDING)
          THEN 1 ELSE 0 END AS breakout_low_24h,
    CASE WHEN price > MAX(price) OVER (PARTITION BY token_address ORDER BY ts_hour
                                       ROWS BETWEEN 72  PRECEDING AND 1 PRECEDING)
          THEN 1 ELSE 0 END AS breakout_high_72h,
    CASE WHEN price < MIN(price) OVER (PARTITION BY token_address ORDER BY ts_hour
                                       ROWS BETWEEN 72  PRECEDING AND 1 PRECEDING)
          THEN 1 ELSE 0 END AS breakout_low_72h,

    -- ── Drawdown from rolling high ────────────────────────────────────────────
    -- 4h: very recent pullback context (entry within pullback vs extension)
    -- 24h/72h/7d: multi-horizon drawdown profile
    price / NULLIF(MAX(price) OVER w4,   0) - 1 AS drawdown_4h,
    price / NULLIF(MAX(price) OVER w24,  0) - 1 AS drawdown_24h,
    price / NULLIF(MAX(price) OVER w72,  0) - 1 AS drawdown_72h,
    price / NULLIF(MAX(price) OVER w168, 0) - 1 AS drawdown_7d,

    -- ── Autocorrelation of returns ────────────────────────────────────────────
    -- 24h: is momentum persisting on a daily basis?
    -- 72h: structural momentum tendency over 3 days
    CORR(ret_1h, ret_1h_lag1) OVER w24 AS acf1_24h,
    CORR(ret_1h, ret_1h_lag1) OVER w72 AS acf1_72h,

    -- ── Volume–return correlation ─────────────────────────────────────────────
    -- Positive = volume higher on up candles (accumulation pattern)
    -- Negative = volume higher on down candles (distribution / panic)
    CORR(ret_1h, volume) OVER w72 AS vol_ret_corr_72h,

    -- ── Signed momentum run ───────────────────────────────────────────────────
    -- Net count of up vs down candles. Extremes = overbought/oversold.
    SUM(CASE WHEN ret_1h > 0 THEN 1 WHEN ret_1h < 0 THEN -1 ELSE 0 END) OVER w4  AS momentum_run_4h,
    SUM(CASE WHEN ret_1h > 0 THEN 1 WHEN ret_1h < 0 THEN -1 ELSE 0 END) OVER w12 AS momentum_run_12h,
    SUM(CASE WHEN ret_1h > 0 THEN 1 WHEN ret_1h < 0 THEN -1 ELSE 0 END) OVER w24 AS momentum_run_24h,

    -- ── Data completeness flags ───────────────────────────────────────────────
    CASE WHEN COUNTIF(ret_1h IS NOT NULL) OVER w24  = 24  THEN 1 ELSE 0 END AS has_24h,
    CASE WHEN COUNTIF(ret_1h IS NOT NULL) OVER w72  = 72  THEN 1 ELSE 0 END AS has_72h,
    CASE WHEN COUNTIF(ret_1h IS NOT NULL) OVER w168 = 168 THEN 1 ELSE 0 END AS has_168h

  FROM rets_with_lag
  WINDOW
    w_unbounded AS (PARTITION BY token_address ORDER BY UNIX_SECONDS(ts_hour) ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW),
    w4   AS (PARTITION BY token_address ORDER BY ts_hour ROWS BETWEEN   3 PRECEDING AND CURRENT ROW),
    w12  AS (PARTITION BY token_address ORDER BY ts_hour ROWS BETWEEN  11 PRECEDING AND CURRENT ROW),
    w24  AS (PARTITION BY token_address ORDER BY ts_hour ROWS BETWEEN  23 PRECEDING AND CURRENT ROW),
    w26  AS (PARTITION BY token_address ORDER BY ts_hour ROWS BETWEEN  25 PRECEDING AND CURRENT ROW),
    w72  AS (PARTITION BY token_address ORDER BY ts_hour ROWS BETWEEN  71 PRECEDING AND CURRENT ROW),
    w168 AS (PARTITION BY token_address ORDER BY ts_hour ROWS BETWEEN 167 PRECEDING AND CURRENT ROW)
),

rsi AS (
  SELECT
    token_address,
    ts_hour,
    price,
    rn,
    ret_1h, logret_1h,
    mean_ret_4h, std_ret_4h,
    mean_ret_12h, std_ret_12h,
    mean_ret_24h, std_ret_24h,
    mean_ret_72h, std_ret_72h,
    mean_ret_168h, std_ret_168h,
    rv_4h, rv_12h, rv_24h, rv_72h, rv_7d,
    sharpe_4h, sharpe_24h, sharpe_72h, sharpe_7d,
    ret_z_24h, ret_z_72h,
    ret_sq_mean_24h, ret_cu_mean_24h,
    cumret_4h, cumret_12h, cumret_24h, cumret_7d,
    sma_4h, sma_12h, sma_24h, sma_72h, sma_168h,
    macd_sma_12_26h,
    price_z_24h, price_z_72h,
    pct_in_range_24h, pct_in_range_72h,
    dist_to_high_4h, dist_to_low_4h,
    dist_to_high_12h, dist_to_low_12h,
    dist_to_high_24h, dist_to_low_24h,
    dist_to_high_72h, dist_to_low_72h,
    breakout_high_4h, breakout_low_4h,
    breakout_high_24h, breakout_low_24h,
    breakout_high_72h, breakout_low_72h,
    drawdown_4h, drawdown_24h, drawdown_72h, drawdown_7d,
    acf1_24h, acf1_72h,
    vol_ret_corr_72h,
    has_24h, has_72h, has_168h,
    volume_ret_1h, volume_ret_24h,
    log_volume, log_volume_per_supply,
    log_volume_mean_24h, log_volume_mean_72h,
    log_volume_mean_24h_per_supply, log_volume_mean_72h_per_supply,
    volume_cv_24h, volume_cv_72h,
    volume_spike_ratio_24h_excl,
    volume_z_24h,
    volume_accel_4v24, volume_accel_24v72,
    volume, total_supply,
    volume_sum_4h, volume_sum_24h, volume_sum_72h,
    volume_mean_24h, volume_mean_72h,
    volume_std_24h, volume_std_72h,
    volume_n_24h,
    volume_ema_fast, volume_ema_slow,
    vwap_4h, vwap_24h,
    amihud_1h, amihud_mean_24h, amihud_mean_72h,
    momentum_run_4h, momentum_run_12h, momentum_run_24h,

    GREATEST(ret_1h,  0) AS gain,
    GREATEST(-ret_1h, 0) AS loss,

    -- RSI-14 (standard)
    AVG(GREATEST( ret_1h, 0)) OVER (PARTITION BY token_address ORDER BY ts_hour ROWS BETWEEN 13 PRECEDING AND CURRENT ROW) AS avg_gain_14,
    AVG(GREATEST(-ret_1h, 0)) OVER (PARTITION BY token_address ORDER BY ts_hour ROWS BETWEEN 13 PRECEDING AND CURRENT ROW) AS avg_loss_14,

    -- RSI-6 (short-term overbought/oversold, more sensitive for 4h moves)
    AVG(GREATEST( ret_1h, 0)) OVER (PARTITION BY token_address ORDER BY ts_hour ROWS BETWEEN 5 PRECEDING AND CURRENT ROW) AS avg_gain_6,
    AVG(GREATEST(-ret_1h, 0)) OVER (PARTITION BY token_address ORDER BY ts_hour ROWS BETWEEN 5 PRECEDING AND CURRENT ROW) AS avg_loss_6
  FROM roll
),

final AS (
  SELECT
    r.token_address,
    r.ts_hour,
    r.price,

    -- ── Returns ───────────────────────────────────────────────────────────────
    COALESCE(r.ret_1h,    0) AS ret_1h,
    COALESCE(r.logret_1h, 0) AS logret_1h,

    -- ── Return statistics ─────────────────────────────────────────────────────
    COALESCE(r.mean_ret_4h,   0) AS mean_ret_4h,
    COALESCE(r.std_ret_4h,    0) AS std_ret_4h,
    COALESCE(r.mean_ret_12h,  0) AS mean_ret_12h,
    COALESCE(r.std_ret_12h,   0) AS std_ret_12h,
    COALESCE(r.mean_ret_24h,  0) AS mean_ret_24h,
    COALESCE(r.std_ret_24h,   0) AS std_ret_24h,
    COALESCE(r.mean_ret_72h,  0) AS mean_ret_72h,
    COALESCE(r.std_ret_72h,   0) AS std_ret_72h,
    COALESCE(r.mean_ret_168h, 0) AS mean_ret_168h,
    COALESCE(r.std_ret_168h,  0) AS std_ret_168h,

    -- ── Realized volatility ───────────────────────────────────────────────────
    COALESCE(r.rv_4h,  0) AS rv_4h,
    COALESCE(r.rv_12h, 0) AS rv_12h,
    COALESCE(r.rv_24h, 0) AS rv_24h,
    COALESCE(r.rv_72h, 0) AS rv_72h,
    COALESCE(r.rv_7d,  0) AS rv_7d,

    -- ── Sharpe ratio ──────────────────────────────────────────────────────────
    COALESCE(r.sharpe_4h,  0) AS sharpe_4h,
    COALESCE(r.sharpe_24h, 0) AS sharpe_24h,
    COALESCE(r.sharpe_72h, 0) AS sharpe_72h,
    COALESCE(r.sharpe_7d,  0) AS sharpe_7d,

    -- ── Return z-scores ───────────────────────────────────────────────────────
    COALESCE(r.ret_z_24h, 0) AS ret_z_24h,
    COALESCE(r.ret_z_72h, 0) AS ret_z_72h,

    -- ── Return skewness (24h) — method of moments ─────────────────────────────
    -- Positive = right tail (occasional large gains)
    -- Negative = left tail (crash risk / rug profile)
    COALESCE(
      SAFE_DIVIDE(
        r.ret_cu_mean_24h
          - 3.0 * r.mean_ret_24h * r.ret_sq_mean_24h
          + 2.0 * POW(r.mean_ret_24h, 3),
        NULLIF(POW(r.std_ret_24h, 3), 0)
      ), 0
    ) AS skew_ret_24h,

    -- ── Cumulative returns ────────────────────────────────────────────────────
    COALESCE(r.cumret_4h,  0) AS cumret_4h,
    COALESCE(r.cumret_12h, 0) AS cumret_12h,
    COALESCE(r.cumret_24h, 0) AS cumret_24h,
    COALESCE(r.cumret_7d,  0) AS cumret_7d,

    -- ── Volume features ───────────────────────────────────────────────────────
    COALESCE(r.volume_ret_1h,  0) AS volume_ret_1h,
    COALESCE(r.volume_ret_24h, 0) AS volume_ret_24h,
    COALESCE(r.log_volume,     0) AS log_volume,
    COALESCE(r.log_volume_per_supply,           0) AS log_volume_per_supply,
    COALESCE(r.log_volume_mean_24h,             0) AS log_volume_mean_24h,
    COALESCE(r.log_volume_mean_72h,             0) AS log_volume_mean_72h,
    COALESCE(r.log_volume_mean_24h_per_supply,  0) AS log_volume_mean_24h_per_supply,
    COALESCE(r.log_volume_mean_72h_per_supply,  0) AS log_volume_mean_72h_per_supply,
    COALESCE(r.volume_cv_24h,                   0) AS volume_cv_24h,
    COALESCE(r.volume_cv_72h,                   0) AS volume_cv_72h,
    COALESCE(r.volume_spike_ratio_24h_excl,     0) AS volume_spike_ratio_24h_excl,
    COALESCE(r.volume_z_24h,                    0) AS volume_z_24h,
    COALESCE(r.volume_accel_4v24,               0) AS volume_accel_4v24,
    COALESCE(r.volume_accel_24v72,              0) AS volume_accel_24v72,
    -- COALESCE(r.volume,       0) AS volume,
    -- COALESCE(r.total_supply, 0) AS total_supply,
    COALESCE(r.volume_sum_4h,    0) AS volume_sum_4h,
    COALESCE(r.volume_sum_24h,   0) AS volume_sum_24h,
    COALESCE(r.volume_sum_72h,   0) AS volume_sum_72h,
    COALESCE(r.volume_mean_24h,  0) AS volume_mean_24h,
    COALESCE(r.volume_mean_72h,  0) AS volume_mean_72h,
    COALESCE(r.volume_std_24h,   0) AS volume_std_24h,
    COALESCE(r.volume_std_72h,   0) AS volume_std_72h,
    COALESCE(r.volume_n_24h,     0) AS volume_n_24h,
    COALESCE(r.volume_ema_fast,  0) AS volume_ema_fast,
    COALESCE(r.volume_ema_slow,  0) AS volume_ema_slow,

    -- ── Price MAs (non-stationary, filtered out by stationary feature mode) ───
    COALESCE(r.sma_4h,   0) AS sma_4h,
    COALESCE(r.sma_12h,  0) AS sma_12h,
    COALESCE(r.sma_24h,  0) AS sma_24h,
    COALESCE(r.sma_72h,  0) AS sma_72h,
    COALESCE(r.sma_168h, 0) AS sma_168h,
    COALESCE(r.macd_sma_12_26h, 0) AS macd_sma_12_26h,

    -- ── Price normalization ───────────────────────────────────────────────────
    COALESCE(r.price_z_24h,      0) AS price_z_24h,
    COALESCE(r.price_z_72h,      0) AS price_z_72h,
    COALESCE(r.pct_in_range_24h, 0) AS pct_in_range_24h,
    COALESCE(r.pct_in_range_72h, 0) AS pct_in_range_72h,

    -- ── MA distances (stationary: normalizes out price level) ─────────────────
    COALESCE(SAFE_DIVIDE(r.price, NULLIF(r.sma_4h,   0)) - 1, 0) AS dist_to_sma_4h,
    COALESCE(SAFE_DIVIDE(r.price, NULLIF(r.sma_12h,  0)) - 1, 0) AS dist_to_sma_12h,
    COALESCE(SAFE_DIVIDE(r.price, NULLIF(r.sma_24h,  0)) - 1, 0) AS dist_to_sma_24h,
    COALESCE(SAFE_DIVIDE(r.price, NULLIF(r.sma_72h,  0)) - 1, 0) AS dist_to_sma_72h,
    COALESCE(SAFE_DIVIDE(r.price, NULLIF(r.sma_168h, 0)) - 1, 0) AS dist_to_sma_168h,

    -- ── VWAP distances ────────────────────────────────────────────────────────
    COALESCE(SAFE_DIVIDE(r.price, NULLIF(r.vwap_4h,  0)) - 1, 0) AS dist_to_vwap_4h,
    COALESCE(SAFE_DIVIDE(r.price, NULLIF(r.vwap_24h, 0)) - 1, 0) AS dist_to_vwap_24h,

    -- ── Local extreme distances ───────────────────────────────────────────────
    COALESCE(r.dist_to_high_4h,  0) AS dist_to_high_4h,
    COALESCE(r.dist_to_low_4h,   0) AS dist_to_low_4h,
    COALESCE(r.dist_to_high_12h, 0) AS dist_to_high_12h,
    COALESCE(r.dist_to_low_12h,  0) AS dist_to_low_12h,
    COALESCE(r.dist_to_high_24h, 0) AS dist_to_high_24h,
    COALESCE(r.dist_to_low_24h,  0) AS dist_to_low_24h,
    COALESCE(r.dist_to_high_72h, 0) AS dist_to_high_72h,
    COALESCE(r.dist_to_low_72h,  0) AS dist_to_low_72h,

    -- ── Breakout flags ────────────────────────────────────────────────────────
    COALESCE(r.breakout_high_4h,  0) AS breakout_high_4h,
    COALESCE(r.breakout_low_4h,   0) AS breakout_low_4h,
    COALESCE(r.breakout_high_24h, 0) AS breakout_high_24h,
    COALESCE(r.breakout_low_24h,  0) AS breakout_low_24h,
    COALESCE(r.breakout_high_72h, 0) AS breakout_high_72h,
    COALESCE(r.breakout_low_72h,  0) AS breakout_low_72h,

    -- ── Drawdown ──────────────────────────────────────────────────────────────
    COALESCE(r.drawdown_4h,  0) AS drawdown_4h,
    COALESCE(r.drawdown_24h, 0) AS drawdown_24h,
    COALESCE(r.drawdown_72h, 0) AS drawdown_72h,
    COALESCE(r.drawdown_7d,  0) AS drawdown_7d,

    -- ── Data quality flags ────────────────────────────────────────────────────
    r.has_24h,
    r.has_72h,
    r.has_168h,

    -- ── RSI ───────────────────────────────────────────────────────────────────
    -- RSI-6: responsive to 4h moves, catches overbought faster than RSI-14
    CASE
      WHEN r.avg_loss_6  IS NULL OR r.avg_loss_6  = 0 THEN 100
      ELSE 100 - 100 / (1 + COALESCE(SAFE_DIVIDE(r.avg_gain_6,  NULLIF(r.avg_loss_6,  0)), 0))
    END AS rsi_6,
    -- RSI-14: standard
    CASE
      WHEN r.avg_loss_14 IS NULL OR r.avg_loss_14 = 0 THEN 100
      ELSE 100 - 100 / (1 + COALESCE(SAFE_DIVIDE(r.avg_gain_14, NULLIF(r.avg_loss_14, 0)), 0))
    END AS rsi_14,

    -- ── Autocorrelation ───────────────────────────────────────────────────────
    COALESCE(r.acf1_24h, 0) AS acf1_24h,
    COALESCE(r.acf1_72h, 0) AS acf1_72h,

    -- ── Volume–return correlation ─────────────────────────────────────────────
    COALESCE(r.vol_ret_corr_72h, 0) AS vol_ret_corr_72h,

    -- ── Amihud illiquidity ────────────────────────────────────────────────────
    COALESCE(r.amihud_1h,       0) AS amihud_1h,
    COALESCE(r.amihud_mean_24h, 0) AS amihud_mean_24h,
    COALESCE(r.amihud_mean_72h, 0) AS amihud_mean_72h,

    -- ── Momentum run ──────────────────────────────────────────────────────────
    COALESCE(r.momentum_run_4h,  0) AS momentum_run_4h,
    COALESCE(r.momentum_run_12h, 0) AS momentum_run_12h,
    COALESCE(r.momentum_run_24h, 0) AS momentum_run_24h,

    -- ── Token age ─────────────────────────────────────────────────────────────
    CAST(r.rn AS FLOAT64) AS token_age_h,

    -- ── Time-of-day / day-of-week ─────────────────────────────────────────────
    EXTRACT(DAYOFWEEK FROM r.ts_hour) AS dow_1_sun_7_sat,
    EXTRACT(HOUR FROM r.ts_hour)      AS hour_of_day,
    SIN(2 * 3.141592653589793 * EXTRACT(HOUR FROM r.ts_hour) / 24.0)                                    AS sin_hour,
    COS(2 * 3.141592653589793 * EXTRACT(HOUR FROM r.ts_hour) / 24.0)                                    AS cos_hour,
    SIN(2 * 3.141592653589793 * CAST(EXTRACT(DAYOFWEEK FROM r.ts_hour) AS FLOAT64) / 7.0)               AS sin_dow,
    COS(2 * 3.141592653589793 * CAST(EXTRACT(DAYOFWEEK FROM r.ts_hour) AS FLOAT64) / 7.0)               AS cos_dow,

    -- ── Volatility ratios ─────────────────────────────────────────────────────
    -- How does current short-term vol compare to longer baselines?
    COALESCE(SAFE_DIVIDE(r.rv_4h,  NULLIF(r.rv_24h, 0)), 0)        AS vol_ratio_4_24,
    COALESCE(SAFE_DIVIDE(r.rv_12h, NULLIF(r.rv_24h, 0)), 0)        AS vol_ratio_12_24,
    COALESCE(SAFE_DIVIDE(r.rv_24h, NULLIF(r.rv_72h, 0)), 0)        AS vol_ratio_24_72,
    COALESCE(SAFE_DIVIDE(r.rv_24h, NULLIF(r.rv_7d,  0)), 0)        AS vol_ratio_24_7d,
    COALESCE(SAFE_DIVIDE(r.std_ret_24h, r.std_ret_72h),  0)        AS vol_ratio_std_24_72,
    COALESCE(SAFE_DIVIDE(r.std_ret_72h, r.std_ret_168h), 0)        AS vol_ratio_std_72_168,

    -- ── Return-over-vol (risk-adjusted short momentum) ────────────────────────
    COALESCE(SAFE_DIVIDE(r.mean_ret_4h,  NULLIF(r.rv_4h,  0)), 0) AS ret_over_rv_4h,
    COALESCE(SAFE_DIVIDE(r.mean_ret_12h, NULLIF(r.rv_12h, 0)), 0) AS ret_over_rv_12h,

    -- ── Volume–price confirmation ─────────────────────────────────────────────
    -- Positive = volume and price direction agree (confirming signal)
    -- Negative = divergence (weakening signal, potential reversal)
    COALESCE(r.ret_1h     * r.volume_ret_1h,    0) AS vol_price_confirm_1h,
    COALESCE(r.mean_ret_24h * r.volume_accel_4v24, 0) AS vol_price_confirm_24h,

    -- ── Per-token slippage proxy ──────────────────────────────────────────────
    -- Derived from 24h realized vol. Calibrated so median-vol token ≈ 35 bps.
    -- Used by the env as a cost column (not a feature), clipped [5, 200] bps.
    LEAST(GREATEST(r.std_ret_24h * 50000.0, 5.0), 200.0) AS slippage_bps_proxy

  FROM rsi r
),

distances_and_slopes AS (
  SELECT
    f.*,

    -- ── MA contrasts ──────────────────────────────────────────────────────────
    COALESCE(f.dist_to_sma_4h  - f.dist_to_sma_24h,  0) AS sma_diff_fast_slow,   -- 4h vs 24h
    COALESCE(f.dist_to_sma_12h - f.dist_to_sma_72h,  0) AS sma_diff_12_72,       -- 12h vs 72h
    COALESCE(f.dist_to_sma_24h - f.dist_to_sma_168h, 0) AS sma_diff_24_168,      -- 24h vs 168h

    -- ── MA slopes (rate of change of price–MA distance per hour) ─────────────
    -- Positive slope = price diverging away from MA (momentum accelerating)
    -- Negative slope = price converging back (momentum fading / mean-reversion)
    COALESCE((f.dist_to_sma_4h  - LAG(f.dist_to_sma_4h,   4) OVER w) /  4, 0) AS sma4h_slope_4h,
    COALESCE((f.dist_to_sma_4h  - LAG(f.dist_to_sma_4h,  12) OVER w) / 12, 0) AS sma4h_slope_12h,
    COALESCE((f.dist_to_sma_12h - LAG(f.dist_to_sma_12h, 12) OVER w) / 12, 0) AS sma12h_slope_12h,
    COALESCE((f.dist_to_sma_12h - LAG(f.dist_to_sma_12h, 24) OVER w) / 24, 0) AS sma12h_slope_24h,
    COALESCE((f.dist_to_sma_24h - LAG(f.dist_to_sma_24h, 24) OVER w) / 24, 0) AS sma24h_slope_24h,
    COALESCE((f.dist_to_sma_24h - LAG(f.dist_to_sma_24h, 72) OVER w) / 72, 0) AS sma24h_slope_72h,
    COALESCE((f.dist_to_sma_72h - LAG(f.dist_to_sma_72h, 72) OVER w) / 72, 0) AS sma72h_slope_72h,

    -- ── Composite indicators ──────────────────────────────────────────────────
    (f.sharpe_24h - f.sharpe_7d)                                     AS sharpe_delta,
    (f.sharpe_4h  - f.sharpe_24h)                                    AS sharpe_delta_short,
    COALESCE(SAFE_DIVIDE(f.rsi_14 * f.vol_ratio_4_24, 100), 0)      AS rsi_vol_interaction

  FROM final f
  WINDOW w AS (PARTITION BY f.token_address ORDER BY f.ts_hour)
),

-- Pre-filter BTC and SOL to one row per ts_hour
btc_dedup AS (
  SELECT * EXCEPT (rn)
  FROM (
    SELECT m.*,
           ROW_NUMBER() OVER (PARTITION BY ts_hour ORDER BY ts_hour) AS rn
    FROM {{ ref('cv_btc_sol_1h') }} m
    WHERE m.token_address = '3NZ9JMVBmGAqocybic2c7LQCJScmgsAZ6vQqTDzcqmJh'
  )
  WHERE rn = 1
),
sol_dedup AS (
  SELECT * EXCEPT (rn)
  FROM (
    SELECT m.*,
           ROW_NUMBER() OVER (PARTITION BY ts_hour ORDER BY ts_hour) AS rn
    FROM {{ ref('cv_btc_sol_1h') }} m
    WHERE m.token_address = 'So11111111111111111111111111111111111111112'
  )
  WHERE rn = 1
),

-- Join token data with BTC and SOL market series
-- Excluded from BTC/SOL: sin/cos hour/dow (identical to token's own time features — pure noise);
-- acf1_72h (BTC autocorrelation is not predictive of altcoin returns);
-- btc_cumret_24h/7d (single-candle raw BTC levels; replaced by cumulative spreads below).
with_market AS (
  SELECT
    f.*,
    -- ── BTC regime context ────────────────────────────────────────────────────
    COALESCE(b.ret_1h, 0)              AS btc_ret_1h,
    COALESCE(b.logret_1h, 0)           AS btc_logret_1h,
    COALESCE(b.mean_ret_24h, 0)        AS btc_mean_ret_24h,
    COALESCE(b.std_ret_24h, 0)         AS btc_std_ret_24h,
    COALESCE(b.mean_ret_72h, 0)        AS btc_mean_ret_72h,
    COALESCE(b.std_ret_72h, 0)         AS btc_std_ret_72h,
    COALESCE(b.mean_ret_168h, 0)       AS btc_mean_ret_168h,
    COALESCE(b.std_ret_168h, 0)        AS btc_std_ret_168h,
    COALESCE(b.rv_24h, 0)              AS btc_rv_24h,
    COALESCE(b.rv_7d, 0)               AS btc_rv_7d,
    -- btc_vol_ratio_24_7d: expanding vol = risk-off, altcoin signals less reliable
    COALESCE(SAFE_DIVIDE(b.rv_24h, NULLIF(b.rv_7d, 0)), 0) AS btc_vol_ratio_24_7d,
    COALESCE(b.sharpe_24h, 0)          AS btc_sharpe_24h,
    COALESCE(b.sharpe_7d, 0)           AS btc_sharpe_7d,
    COALESCE(b.ret_z_24h, 0)           AS btc_ret_z_24h,
    COALESCE(b.macd_sma_12_26h, 0)     AS btc_macd_sma_12_26h,
    COALESCE(b.dist_to_sma_12h, 0)     AS btc_dist_to_sma_12h,
    COALESCE(b.dist_to_sma_24h, 0)     AS btc_dist_to_sma_24h,
    COALESCE(b.dist_to_sma_168h, 0)    AS btc_dist_to_sma_168h,
    COALESCE(b.pct_in_range_24h, 0)    AS btc_pct_in_range_24h,
    COALESCE(b.dist_to_high_24h, 0)    AS btc_dist_to_high_24h,
    COALESCE(b.dist_to_low_24h, 0)     AS btc_dist_to_low_24h,
    COALESCE(b.breakout_high_24h, 0)   AS btc_breakout_high_24h,
    COALESCE(b.breakout_low_24h, 0)    AS btc_breakout_low_24h,
    COALESCE(b.drawdown_7d, 0)         AS btc_drawdown_7d,
    COALESCE(b.rsi_14, 0)              AS btc_rsi_14,
    COALESCE(b.sharpe_delta, 0)        AS btc_sharpe_delta,
    -- Single-candle spreads (token vs BTC)
    COALESCE(f.ret_1h    - b.ret_1h,    0) AS spread_ret_1h,
    COALESCE(f.logret_1h - b.logret_1h, 0) AS spread_logret_1h,
    -- ── SOL regime context ────────────────────────────────────────────────────
    COALESCE(s.ret_1h, 0)              AS sol_ret_1h,
    COALESCE(s.logret_1h, 0)           AS sol_logret_1h,
    COALESCE(s.mean_ret_24h, 0)        AS sol_mean_ret_24h,
    COALESCE(s.std_ret_24h, 0)         AS sol_std_ret_24h,
    COALESCE(s.mean_ret_72h, 0)        AS sol_mean_ret_72h,
    COALESCE(s.std_ret_72h, 0)         AS sol_std_ret_72h,
    COALESCE(s.mean_ret_168h, 0)       AS sol_mean_ret_168h,
    COALESCE(s.std_ret_168h, 0)        AS sol_std_ret_168h,
    COALESCE(s.rv_24h, 0)              AS sol_rv_24h,
    COALESCE(s.rv_7d, 0)               AS sol_rv_7d,
    COALESCE(SAFE_DIVIDE(s.rv_24h, NULLIF(s.rv_7d, 0)), 0) AS sol_vol_ratio_24_7d,
    COALESCE(s.sharpe_24h, 0)          AS sol_sharpe_24h,
    COALESCE(s.sharpe_7d, 0)           AS sol_sharpe_7d,
    COALESCE(s.ret_z_24h, 0)           AS sol_ret_z_24h,
    COALESCE(s.cumret_24h, 0)          AS sol_cumret_24h,
    COALESCE(s.cumret_7d, 0)           AS sol_cumret_7d,
    COALESCE(s.macd_sma_12_26h, 0)     AS sol_macd_sma_12_26h,
    COALESCE(s.dist_to_sma_12h, 0)     AS sol_dist_to_sma_12h,
    COALESCE(s.dist_to_sma_24h, 0)     AS sol_dist_to_sma_24h,
    COALESCE(s.dist_to_sma_168h, 0)    AS sol_dist_to_sma_168h,
    COALESCE(s.pct_in_range_24h, 0)    AS sol_pct_in_range_24h,
    COALESCE(s.dist_to_high_24h, 0)    AS sol_dist_to_high_24h,
    COALESCE(s.dist_to_low_24h, 0)     AS sol_dist_to_low_24h,
    COALESCE(s.breakout_high_24h, 0)   AS sol_breakout_high_24h,
    COALESCE(s.breakout_low_24h, 0)    AS sol_breakout_low_24h,
    COALESCE(s.drawdown_7d, 0)         AS sol_drawdown_7d,
    COALESCE(s.rsi_14, 0)              AS sol_rsi_14,
    COALESCE(s.sharpe_delta, 0)        AS sol_sharpe_delta,
    -- Single-candle spreads (token vs SOL)
    COALESCE(f.ret_1h    - s.ret_1h,    0) AS sol_spread_ret_1h,
    COALESCE(f.logret_1h - s.logret_1h, 0) AS sol_spread_logret_1h
  FROM distances_and_slopes f
  LEFT JOIN btc_dedup b ON f.ts_hour = b.ts_hour
  LEFT JOIN sol_dedup s ON f.ts_hour = s.ts_hour
),

-- Cross-asset features that require window functions over the joined series.
-- These are only computable after the join because they need both token and
-- BTC/SOL returns in the same row.
cross_asset AS (
  SELECT
    m.*,

    -- ── BTC beta (rolling 72h sensitivity to BTC moves) ───────────────────────
    -- beta = COVAR(token, btc) / VAR(btc)
    -- Using: COVAR(x,y) = AVG(x·y) − AVG(x)·AVG(y)
    COALESCE(SAFE_DIVIDE(
      AVG(m.ret_1h * m.btc_ret_1h) OVER w72
        - AVG(m.ret_1h)     OVER w72 * AVG(m.btc_ret_1h) OVER w72,
      NULLIF(POW(STDDEV_SAMP(m.btc_ret_1h) OVER w72, 2), 0)
    ), 0) AS btc_beta_72h,

    -- ── Alpha return (idiosyncratic move stripped of BTC noise) ───────────────
    -- alpha_ret_1h > 0 → outperforming BTC on a risk-adjusted basis
    -- alpha_ret_1h < 0 → underperforming (token lagging BTC)
    COALESCE(
      m.ret_1h - SAFE_DIVIDE(
        AVG(m.ret_1h * m.btc_ret_1h) OVER w72
          - AVG(m.ret_1h) OVER w72 * AVG(m.btc_ret_1h) OVER w72,
        NULLIF(POW(STDDEV_SAMP(m.btc_ret_1h) OVER w72, 2), 0)
      ) * m.btc_ret_1h
    , 0) AS alpha_ret_1h,

    -- ── BTC cumulative spread (sustained divergence > single-candle noise) ────
    -- spread_cumret_4h > 0 → token outperformed BTC over last 4h
    COALESCE(m.cumret_4h  - (EXP(SUM(m.btc_logret_1h) OVER w4)  - 1), 0) AS spread_cumret_4h,
    COALESCE(m.cumret_24h - (EXP(SUM(m.btc_logret_1h) OVER w24) - 1), 0) AS spread_cumret_24h,

    -- ── SOL cumulative spread ─────────────────────────────────────────────────
    COALESCE(m.cumret_4h  - (EXP(SUM(m.sol_logret_1h) OVER w4)  - 1), 0) AS sol_spread_cumret_4h,
    COALESCE(m.cumret_24h - (EXP(SUM(m.sol_logret_1h) OVER w24) - 1), 0) AS sol_spread_cumret_24h

  FROM with_market m
  WINDOW
    w4  AS (PARTITION BY m.token_address ORDER BY m.ts_hour ROWS BETWEEN  3 PRECEDING AND CURRENT ROW),
    w24 AS (PARTITION BY m.token_address ORDER BY m.ts_hour ROWS BETWEEN 23 PRECEDING AND CURRENT ROW),
    w72 AS (PARTITION BY m.token_address ORDER BY m.ts_hour ROWS BETWEEN 71 PRECEDING AND CURRENT ROW)
)

SELECT *
FROM cross_asset
