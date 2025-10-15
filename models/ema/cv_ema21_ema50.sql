WITH base AS (
  SELECT
    c.token_address,
    c.monitoring_session_id,
    TIMESTAMP_TRUNC(c.price_timestamp, HOUR) AS ts_hour,
    AVG(SAFE_CAST(c.price_usd AS FLOAT64)) AS price,
    SAFE_CAST(c.volume AS FLOAT64) AS volume,
    SAFE_CAST(t.circSupply AS FLOAT64) AS total_supply
  FROM {{ ref('cv_filter_prep_72_ext_windows') }} c
  LEFT JOIN {{ source('core', 'token_metadata_jup_tmp') }} t
    ON c.token_address = t.id
    AND t.organicScoreLabel <> 'low'
    AND t.mcap >= 100000000
  GROUP BY
    c.token_address, c.monitoring_session_id, ts_hour, c.volume, t.circSupply
),

-- Row index per token (no session reset; add session if you want resets)
idx AS (
  SELECT
    b.*,
    ROW_NUMBER() OVER (PARTITION BY token_address ORDER BY ts_hour) AS rn
  FROM base b
),

-- Precompute r^(-rn) terms (r = 1 - 2/(N+1))
pre AS (
  SELECT
    *,
    POW(1 - 2.0/22.0, -rn) AS w21,   -- r21^(-rn)
    POW(1 - 2.0/51.0, -rn) AS w50    -- r50^(-rn)
  FROM idx
),

-- Cumulative sums of price * r^(-rn) and r^(-rn)
ema AS (
  SELECT
    p.*,
    -- EMA 21
    ( SUM(price * w21) OVER (PARTITION BY token_address ORDER BY ts_hour
        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
    /
      SUM(w21)          OVER (PARTITION BY token_address ORDER BY ts_hour
        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
    ) AS ema_21,

    -- EMA 50
    ( SUM(price * w50) OVER (PARTITION BY token_address ORDER BY ts_hour
        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
    /
      SUM(w50)          OVER (PARTITION BY token_address ORDER BY ts_hour
        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
    ) AS ema_50
  FROM pre p
),

-- Flags and time-based lookbacks (exclude current bar using ts_prev = ts_hour - 1s)
flags AS (
  SELECT
    e.*,
    SAFE_CAST(ema_21 > ema_50 AS INT64) AS gt_flag,
    SAFE_CAST(ema_21 < ema_50 AS INT64) AS lt_flag,
    TIMESTAMP_SUB(ts_hour, INTERVAL 1 SECOND) AS ts_prev
  FROM ema e
)

SELECT
  token_address,
  monitoring_session_id,
  ts_hour,
  price,
  ema_21,
  ema_50,
  (ema_21 - ema_50) AS delta_ema,
  gt_flag,
  lt_flag,

  -- Past 6h
  MAX(gt_flag) OVER (PARTITION BY token_address ORDER BY UNIX_SECONDS(ts_prev)
                     RANGE BETWEEN 21600 PRECEDING AND CURRENT ROW) AS gt_any_6h,
  MIN(gt_flag) OVER (PARTITION BY token_address ORDER BY UNIX_SECONDS(ts_prev)
                     RANGE BETWEEN 21600 PRECEDING AND CURRENT ROW) AS gt_all_6h,
  AVG(gt_flag) OVER (PARTITION BY token_address ORDER BY UNIX_SECONDS(ts_prev)
                     RANGE BETWEEN 21600 PRECEDING AND CURRENT ROW) AS gt_frac_6h,

  -- Past 24h
  MAX(gt_flag) OVER (PARTITION BY token_address ORDER BY UNIX_SECONDS(ts_prev)
                     RANGE BETWEEN 86400 PRECEDING AND CURRENT ROW) AS gt_any_24h,
  MIN(gt_flag) OVER (PARTITION BY token_address ORDER BY UNIX_SECONDS(ts_prev)
                     RANGE BETWEEN 86400 PRECEDING AND CURRENT ROW) AS gt_all_24h,
  AVG(gt_flag) OVER (PARTITION BY token_address ORDER BY UNIX_SECONDS(ts_prev)
                     RANGE BETWEEN 86400 PRECEDING AND CURRENT ROW) AS gt_frac_24h,

  -- Past 168h
  MAX(gt_flag) OVER (PARTITION BY token_address ORDER BY UNIX_SECONDS(ts_prev)
                     RANGE BETWEEN 604800 PRECEDING AND CURRENT ROW) AS gt_any_168h,
  MIN(gt_flag) OVER (PARTITION BY token_address ORDER BY UNIX_SECONDS(ts_prev)
                     RANGE BETWEEN 604800 PRECEDING AND CURRENT ROW) AS gt_all_168h,
  AVG(gt_flag) OVER (PARTITION BY token_address ORDER BY UNIX_SECONDS(ts_prev)
                     RANGE BETWEEN 604800 PRECEDING AND CURRENT ROW) AS gt_frac_168h

FROM flags
ORDER BY token_address, ts_hour
