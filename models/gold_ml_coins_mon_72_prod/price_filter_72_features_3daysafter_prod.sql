{{ config(
    schema='gold_ml_coins_mon_72_prod',
    materialized='view'
) }}

-- 1) Hourly prices from the extended windows (keep session metadata & flags)
WITH hourly AS (
  SELECT
    address AS token_address,
    TIMESTAMP_TRUNC(datetime, HOUR) AS ts_hour,
    AVG(CAST(price AS FLOAT64)) AS price
  FROM {{ source('streamed_datapublic', 'historical_prices') }}
  WHERE price IS NOT NULL
  GROUP BY
    token_address, ts_hour
),

-- 2) Candidate acquisition timestamps
acquisitions AS (
  SELECT
    token_address,
    ts_hour AS first_acquired_timestamp
  FROM hourly
),

-- 3) Build the 0..168h window for each acquisition by joining forward in time.
base AS (
  SELECT
    a.token_address,
    a.first_acquired_timestamp,
    h.ts_hour,
    CAST(h.price AS FLOAT64) AS price,
    -- hours since entry
    CAST(TIMESTAMP_DIFF(h.ts_hour, a.first_acquired_timestamp, HOUR) AS INT64) AS h
  FROM acquisitions a
  JOIN hourly h
    ON h.token_address = a.token_address
   AND h.ts_hour BETWEEN a.first_acquired_timestamp AND TIMESTAMP_ADD(a.first_acquired_timestamp, INTERVAL 72 HOUR)
),

-- 4) Per-series sequential metrics (compute returns, running max, etc.)
seq AS (
  SELECT
    b.*,
    -- price at entry (h = 0)
    MAX(IF(b.h = 0, b.price, NULL)) OVER (PARTITION BY b.token_address,b.first_acquired_timestamp) AS price0,

    -- add an explicit lag
    LAG(b.price) OVER (
      PARTITION BY b.token_address, b.first_acquired_timestamp
      ORDER BY b.h
    ) AS price_lag1,

    -- safe 1h log return (no LN(0))
    CASE
      WHEN b.price > 0 AND LAG(b.price) OVER (
        PARTITION BY b.token_address, b.first_acquired_timestamp
        ORDER BY b.h
      ) > 0
      THEN LOG(b.price) - LOG(LAG(b.price) OVER (
        PARTITION BY b.token_address, b.first_acquired_timestamp
        ORDER BY b.h
      ))
      ELSE NULL
    END AS logret_1h,

    -- cumulative return from the entry price
    SAFE_DIVIDE(b.price, MAX(IF(b.h = 0, b.price, NULL)) OVER (PARTITION BY b.token_address, b.first_acquired_timestamp)) - 1
      AS cumret_from_entry,

    -- running peak for drawdowns
    MAX(b.price) OVER (
      PARTITION BY b.token_address, b.first_acquired_timestamp
      ORDER BY b.h
      ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) AS run_max_price,

    STRUCT(b.price AS price, b.h AS h) AS price_h_struct
  FROM base b
),

-- 5) Aggregate per acquisition (one row per entry)
per_series AS (
  SELECT
    token_address,
    first_acquired_timestamp,
    -- completeness
    COUNT(*) AS n_points,
    MAX(h)   AS max_h,
    CASE WHEN COUNT(*) >= 73 AND MAX(h) = 72 AND MIN(h) = 0 THEN 1 ELSE 0 END AS has_full_3d,

    -- entry price
    ANY_VALUE(price0) AS price0,

    -- end states & daily checkpoints (NULL-safe -> default 0)
    COALESCE(MAX(IF(h = 8,  SAFE_DIVIDE(price, price0) - 1, NULL)), 0)  AS ret_8h,   -- EDIT
    COALESCE(MAX(IF(h = 24,  SAFE_DIVIDE(price, price0) - 1, NULL)), 0)  AS ret_24h,   -- EDIT
    COALESCE(MAX(IF(h = 48,  SAFE_DIVIDE(price, price0) - 1, NULL)), 0)  AS ret_48h,   -- EDIT
    COALESCE(MAX(IF(h = 72,  SAFE_DIVIDE(price, price0) - 1, NULL)), 0)  AS ret_72h,   -- EDIT


    -- kinetics: first time to hit thresholds (already default 200)
    COALESCE(MIN(IF(cumret_from_entry >= 0.10, h, NULL)), 200) AS t_hit_up_10,
    COALESCE(MIN(IF(cumret_from_entry >= 0.15, h, NULL)), 200) AS t_hit_up_15,
    COALESCE(MIN(IF(cumret_from_entry >= 0.20, h, NULL)), 200) AS t_hit_up_20,
    COALESCE(MIN(IF(cumret_from_entry >= 0.25, h, NULL)), 200) AS t_hit_up_25,
    COALESCE(MIN(IF(cumret_from_entry >= 0.30, h, NULL)), 200) AS t_hit_up_30,
    COALESCE(MIN(IF(cumret_from_entry >= 0.35, h, NULL)), 200) AS t_hit_up_35,
    COALESCE(MIN(IF(cumret_from_entry >= 0.40, h, NULL)), 200) AS t_hit_up_40,
    COALESCE(MIN(IF(cumret_from_entry >= 0.45, h, NULL)), 200) AS t_hit_up_45,
    COALESCE(MIN(IF(cumret_from_entry >= 0.50, h, NULL)), 200) AS t_hit_up_50,
    COALESCE(MIN(IF(cumret_from_entry >= 0.75, h, NULL)), 200) AS t_hit_up_75,
    COALESCE(MIN(IF(cumret_from_entry >= 1.00, h, NULL)), 200) AS t_hit_up_100,
    COALESCE(MIN(IF(cumret_from_entry >= 2.00, h, NULL)), 200) AS t_hit_up_200,
    COALESCE(MIN(IF(cumret_from_entry >= 5.00, h, NULL)), 200) AS t_hit_up_500,

    COALESCE(MIN(IF(cumret_from_entry <= -0.10, h, NULL)), 200) AS t_hit_dn_10,
    COALESCE(MIN(IF(cumret_from_entry <= -0.15, h, NULL)), 200) AS t_hit_dn_15,
    COALESCE(MIN(IF(cumret_from_entry <= -0.20, h, NULL)), 200) AS t_hit_dn_20,
    COALESCE(MIN(IF(cumret_from_entry <= -0.25, h, NULL)), 200) AS t_hit_dn_25,
    COALESCE(MIN(IF(cumret_from_entry <= -0.30, h, NULL)), 200) AS t_hit_dn_30,
    COALESCE(MIN(IF(cumret_from_entry <= -0.35, h, NULL)), 200) AS t_hit_dn_35,
    COALESCE(MIN(IF(cumret_from_entry <= -0.40, h, NULL)), 200) AS t_hit_dn_40,
    COALESCE(MIN(IF(cumret_from_entry <= -0.45, h, NULL)), 200) AS t_hit_dn_45,
    COALESCE(MIN(IF(cumret_from_entry <= -0.50, h, NULL)), 200) AS t_hit_dn_50,

    -- best/worst points and when they happen within 3d
    (ARRAY_AGG(price_h_struct ORDER BY price DESC, h ASC LIMIT 1))[OFFSET(0)].price AS peak_price_3d,
    (ARRAY_AGG(price_h_struct ORDER BY price DESC, h ASC LIMIT 1))[OFFSET(0)].h     AS t_peak_h,
    (ARRAY_AGG(price_h_struct ORDER BY price ASC,  h ASC LIMIT 1))[OFFSET(0)].price AS trough_price_3d,
    (ARRAY_AGG(price_h_struct ORDER BY price ASC,  h ASC LIMIT 1))[OFFSET(0)].h     AS t_trough_h,

    -- max gain from entry & max drawdown vs running peak
    MAX(SAFE_DIVIDE(price, price0) - 1) AS max_gain_from_entry_3d,
    MIN(SAFE_DIVIDE(price, run_max_price) - 1) AS max_drawdown_3d,

    -- realized volatility & hourly distribution
    SQRT(SUM(POW(COALESCE(logret_1h, 0), 2))) * SQRT(24) AS rv_3d,
    STDDEV_SAMP(logret_1h) AS std_logret_1h_3d,
    AVG(logret_1h)         AS mean_logret_1h_3d,

    -- tail risk
    APPROX_QUANTILES(ABS(logret_1h), 100)[OFFSET(95)] AS p95_abs_logret_1h,

    -- directional persistence (NULL-safe -> default 0)
    COALESCE(
      SAFE_DIVIDE(
        SUM(CASE WHEN logret_1h > 0 THEN 1 ELSE 0 END),
        NULLIF(SUM(CASE WHEN logret_1h IS NOT NULL THEN 1 ELSE 0 END), 0)
      ),
      0
    ) AS pct_pos_hours, -- EDIT

    -- rough Sharpe-like over the 3d path (mean/std * sqrt(24))
    SAFE_DIVIDE(AVG(logret_1h), NULLIF(STDDEV_SAMP(logret_1h), 0)) * SQRT(24) AS sharpe_like_3d,

    -- trend of log price: slope per hour & R^2
    SAFE_DIVIDE(
      COVAR_SAMP(LOG(NULLIF(price, 0)), CAST(h AS FLOAT64)),
      VAR_SAMP(CAST(h AS FLOAT64))
    ) AS slope_logprice_per_h,
    POW(CORR(CAST(h AS FLOAT64), LOG(NULLIF(price, 0))), 2) AS r2_logprice_trend,

    -- shape features (NULL-safe -> default 0)
    COALESCE(AVG(cumret_from_entry), 0) AS auc_cumret_avg,                        -- EDIT
    COALESCE(AVG(IF(h BETWEEN 0  AND 8,  cumret_from_entry, NULL)), 0) AS avg_cumret_0_8h,   -- EDIT
    COALESCE(AVG(IF(h BETWEEN 8  AND 24,  cumret_from_entry, NULL)), 0) AS avg_cumret_8_24h,   -- EDIT
    COALESCE(AVG(IF(h BETWEEN 24 AND 48,  cumret_from_entry, NULL)), 0) AS avg_cumret_24_48h,   -- EDIT
    COALESCE(AVG(IF(h BETWEEN 48 AND 72,  cumret_from_entry, NULL)), 0) AS avg_cumret_48_72h,  -- EDIT
  FROM seq
  GROUP BY
    token_address, first_acquired_timestamp
)

-- 6) Final projection (ensure remaining fields are NULL-safe)
SELECT
  token_address,
  first_acquired_timestamp,

  -- completeness
  n_points, max_h, has_full_3d,

  -- end states & daily checkpoints
  ret_8h,ret_24h, ret_48h, ret_72h,

  -- kinetics (hours to thresholds)
  t_hit_up_10, t_hit_up_15, t_hit_up_20, t_hit_up_25, t_hit_up_30, t_hit_up_35, t_hit_up_40, t_hit_up_45, t_hit_up_50, t_hit_up_100, t_hit_up_200, t_hit_up_500,
  t_hit_dn_10, t_hit_dn_15, t_hit_dn_20, t_hit_dn_25, t_hit_dn_30, t_hit_dn_35, t_hit_dn_40, t_hit_dn_45, t_hit_dn_50,

  -- extremes
  COALESCE(max_gain_from_entry_3d, 0) AS max_gain_from_entry_3d,  -- EDIT (paranoia)
  COALESCE(max_drawdown_3d, 0)       AS max_drawdown_3d,          -- EDIT (paranoia)
  COALESCE(t_peak_h, -1)             AS t_peak_h,                 -- EDIT
  COALESCE(t_trough_h, -1)           AS t_trough_h,               -- EDIT

  -- volatility & tail
  COALESCE(rv_3d, 0)                 AS rv_3d,                    -- EDIT (paranoia)
  COALESCE(std_logret_1h_3d,  0)     AS std_logret_1h_3d,
  COALESCE(mean_logret_1h_3d, 0)     AS mean_logret_1h_3d,
  COALESCE(p95_abs_logret_1h, 0)     AS p95_abs_logret_1h,
  COALESCE(pct_pos_hours, 0)         AS pct_pos_hours,            -- EDIT
  COALESCE(sharpe_like_3d, 0)        AS sharpe_like_3d,

  -- trend & shape
  COALESCE(slope_logprice_per_h, 0)  AS slope_logprice_per_h,
  COALESCE(r2_logprice_trend,   0)   AS r2_logprice_trend,
  COALESCE(auc_cumret_avg, 0)        AS auc_cumret_avg,           -- EDIT
  COALESCE(avg_cumret_0_8h, 0)      AS avg_cumret_0_8h,         -- EDIT
  COALESCE(avg_cumret_8_24h, 0)     AS avg_cumret_8_24h,        -- EDIT
  COALESCE(avg_cumret_24_48h, 0)    AS avg_cumret_24_48h,       -- EDIT
  COALESCE(avg_cumret_48_72h, 0)    AS avg_cumret_48_72h,       -- EDIT
  CASE
    WHEN t_hit_up_35 < 75 AND t_hit_up_35 < t_hit_dn_25 THEN 1
    ELSE 0
  END AS label_profit35_before_loss25,

  CASE
    WHEN t_hit_up_20 < 75 AND t_hit_up_20 < t_hit_dn_25 THEN 1
    ELSE 0
  END AS label_profit20_before_loss25,

  CASE
    WHEN t_hit_up_15 < 75 AND t_hit_up_15 < t_hit_dn_20 THEN 1
    ELSE 0
  END AS label_profit15_before_loss20,

  CASE
    WHEN t_hit_up_25 < 75 AND t_hit_up_25 < t_hit_dn_25 THEN 1
    ELSE 0
  END AS label_profit25_before_loss25,

  CASE 
    WHEN t_hit_up_10 < 75 AND t_hit_up_10 < t_hit_dn_15 THEN 1
    ELSE 0
  END AS label_profit10_before_loss15,

FROM per_series
ORDER BY token_address, first_acquired_timestamp
