{{ config(materialized='table') }}

WITH base AS (
  SELECT
    s.chain,
    s.token_address,
    s.price_timestamp,
    s.open,
    s.high,
    s.low,
    s.close,
    s.volume,

    -- Candle geometry
    (s.high - s.low) AS rangee,
    ABS(s.close - s.open) AS body,
    SAFE_DIVIDE(
      s.high - GREATEST(s.open, s.close),
      NULLIF(s.high - s.low, 0)
    ) AS upper_wick_frac,
    SAFE_DIVIDE(
      LEAST(s.open, s.close) - s.low,
      NULLIF(s.high - s.low, 0)
    ) AS lower_wick_frac,
    SAFE_DIVIDE(ABS(s.close - s.open), NULLIF(s.high - s.low, 0)) AS body_frac,

    -- Direction
    s.close >= s.open AS is_green,

    -- 1-bar return vs previous close (per token on each chain)
    SAFE_DIVIDE(
      s.close
        - LAG(s.close) OVER (
            PARTITION BY s.chain, s.token_address
            ORDER BY s.price_timestamp
          ),
      NULLIF(
        LAG(s.close) OVER (
          PARTITION BY s.chain, s.token_address
          ORDER BY s.price_timestamp
        ),
        0
      )
    ) AS ret_from_prev
  FROM {{ ref('token_ohlcv') }} AS s
  -- hourlies assumed; if you also have other intervals, filter here
  WHERE s.price_timestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 90 DAY)
),

agg AS (
  SELECT
    chain,
    token_address,
    COUNT(*) AS n,

    -- Price distribution
    APPROX_QUANTILES(close, 1001)[OFFSET(500)] AS median_close,
    STDDEV_SAMP(close) AS close_std,
    MIN(close) AS min_close,
    MAX(close) AS max_close,
    APPROX_QUANTILES(close, 1001)[OFFSET(10)]  AS p01_close,
    APPROX_QUANTILES(close, 1001)[OFFSET(990)] AS p99_close,

    -- Lows distribution
    APPROX_QUANTILES(low, 1001)[OFFSET(10)] AS p01_low,
    MIN(low) AS min_low,

    -- Volume stats
    AVG(volume) AS vol_avg,
    STDDEV_SAMP(volume) AS vol_std,
    MAX(volume) AS max_volume,
    APPROX_QUANTILES(volume, 1001)[OFFSET(500)] AS median_volume,
    AVG(CASE WHEN volume IS NULL OR volume = 0 THEN 1.0 ELSE 0.0 END) AS zero_vol_share,

    -- Candle shape
    AVG(rangee) AS avg_range,
    AVG(body)  AS avg_body,
    AVG(body_frac) AS avg_body_frac,
    AVG(
      CASE
        WHEN rangee > 0 AND body_frac <= 0.15 THEN 1.0
        ELSE 0.0
      END
    ) AS wickiness_share,

    -- Trend-ish features
    AVG(CASE WHEN is_green THEN 1.0 ELSE 0.0 END) AS green_share,
    AVG(
      CASE
        WHEN ret_from_prev IS NULL THEN 1.0
        WHEN ABS(ret_from_prev) < 0.002 THEN 1.0       -- < 0.2% move
        ELSE 0.0
      END
    ) AS tiny_move_share,
    AVG(
      CASE
        WHEN ret_from_prev IS NULL THEN 0.0
        WHEN ABS(ret_from_prev) > 0.30 THEN 1.0        -- > 30% move
        ELSE 0.0
      END
    ) AS big_move_share,
    MAX(ABS(ret_from_prev)) AS max_abs_ret,

    MAX(price_timestamp) AS last_ts
  FROM base
  GROUP BY chain, token_address
)

SELECT
  *,
  -- some normalized ratios to reuse downstream
  SAFE_DIVIDE(close_std, median_close) AS close_vol_ratio,
  SAFE_DIVIDE(avg_range, median_close) AS range_ratio,
  SAFE_DIVIDE(p01_low, median_close)   AS p01_low_ratio,
  SAFE_DIVIDE(min_low, median_close)   AS min_low_ratio,
  SAFE_DIVIDE(max_close, min_close)    AS max_to_min_ratio,
  SAFE_DIVIDE(p99_close, p01_close)    AS p99_to_p01_close_ratio,
  SAFE_DIVIDE(max_volume, NULLIF(median_volume, 0)) AS max_to_med_vol_ratio
FROM agg
WHERE n >= 500
