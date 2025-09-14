{{ config(
    materialized='table'
) }}

{# -----------------------------------------------------------
   Build forward 0..168h (7d) path features for labeling,
   using price_filter_prep_ext_windows as the source.
   Each core-monitoring hour becomes a potential entry.
   ----------------------------------------------------------- #}

-- 1) Hourly prices from the extended windows (keep session metadata & flags)
WITH hourly AS (
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
    extended_start, extended_end, in_pre_extension, in_core_monitoring, in_post_extension, ts_hour
),

-- 2) Candidate acquisition timestamps: all core-monitoring hours that still have room
--    for a full 7-day lookahead within the same session's extended window.
acquisitions AS (
  SELECT
    token_address,
    monitoring_session_id,
    session_start,
    session_end,
    extended_start,
    extended_end,
    ts_hour AS first_acquired_timestamp
  FROM hourly
  WHERE in_core_monitoring = TRUE
    -- ensure the forward window stays inside the session's extended_end
    AND TIMESTAMP_ADD(ts_hour, INTERVAL 168 HOUR) <= extended_end
),

-- 3) Build the 0..168h window for each acquisition by joining forward in time.
base AS (
  SELECT
    a.token_address,
    a.monitoring_session_id,
    a.session_start,
    a.session_end,
    a.extended_start,
    a.extended_end,

    a.first_acquired_timestamp,

    h.ts_hour,
    CAST(h.price AS FLOAT64) AS price,

    -- hours since entry
    CAST(TIMESTAMP_DIFF(h.ts_hour, a.first_acquired_timestamp, HOUR) AS INT64) AS h
  FROM acquisitions a
  JOIN hourly h
    ON h.token_address           = a.token_address
   AND h.monitoring_session_id    = a.monitoring_session_id
   AND h.ts_hour BETWEEN a.first_acquired_timestamp AND TIMESTAMP_ADD(a.first_acquired_timestamp, INTERVAL 168 HOUR)
),

-- 4) Per-series sequential metrics (compute returns, running max, etc.)
seq AS (
  SELECT
    b.*,

    -- price at entry (h = 0)
    MAX(IF(b.h = 0, b.price, NULL)) OVER (PARTITION BY b.token_address, b.monitoring_session_id, b.first_acquired_timestamp) AS price0,

    -- add an explicit lag
    LAG(b.price) OVER (
      PARTITION BY b.token_address, b.monitoring_session_id, b.first_acquired_timestamp
      ORDER BY b.h
    ) AS price_lag1,

    -- safe 1h log return (no LN(0))
    CASE
      WHEN b.price > 0 AND LAG(b.price) OVER (
        PARTITION BY b.token_address, b.monitoring_session_id, b.first_acquired_timestamp
        ORDER BY b.h
      ) > 0
      THEN LOG(b.price) - LOG(LAG(b.price) OVER (
        PARTITION BY b.token_address, b.monitoring_session_id, b.first_acquired_timestamp
        ORDER BY b.h
      ))
      ELSE NULL
    END AS logret_1h,

    -- cumulative return from the entry price
    SAFE_DIVIDE(b.price, MAX(IF(b.h = 0, b.price, NULL)) OVER (PARTITION BY b.token_address, b.monitoring_session_id, b.first_acquired_timestamp)) - 1
      AS cumret_from_entry,

    -- running peak for drawdowns
    MAX(b.price) OVER (
      PARTITION BY b.token_address, b.monitoring_session_id, b.first_acquired_timestamp
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
    monitoring_session_id,
    first_acquired_timestamp,
    session_start,
    session_end,
    extended_start,
    extended_end,

    -- completeness
    COUNT(*) AS n_points,
    MAX(h)   AS max_h,
    CASE WHEN COUNT(*) >= 169 AND MAX(h) = 168 AND MIN(h) = 0 THEN 1 ELSE 0 END AS has_full_7d,

    -- entry price
    ANY_VALUE(price0) AS price0,

    -- end states & daily checkpoints (returns from entry at specific hours)
    MAX(IF(h = 24,  SAFE_DIVIDE(price, price0) - 1, NULL))  AS ret_24h,
    MAX(IF(h = 48,  SAFE_DIVIDE(price, price0) - 1, NULL))  AS ret_48h,
    MAX(IF(h = 72,  SAFE_DIVIDE(price, price0) - 1, NULL))  AS ret_72h,
    MAX(IF(h = 96,  SAFE_DIVIDE(price, price0) - 1, NULL))  AS ret_96h,
    MAX(IF(h = 120, SAFE_DIVIDE(price, price0) - 1, NULL))  AS ret_120h,
    MAX(IF(h = 144, SAFE_DIVIDE(price, price0) - 1, NULL))  AS ret_144h,
    MAX(IF(h = 168, SAFE_DIVIDE(price, price0) - 1, NULL))  AS ret_168h,

    -- kinetics: first time to hit thresholds (in hours; fallback=200 if never hit)
    COALESCE(MIN(IF(cumret_from_entry >= 0.10, h, NULL)), 200) AS t_hit_up_10,
    COALESCE(MIN(IF(cumret_from_entry >= 0.15, h, NULL)), 200) AS t_hit_up_15,
    COALESCE(MIN(IF(cumret_from_entry >= 0.20, h, NULL)), 200) AS t_hit_up_20,
    COALESCE(MIN(IF(cumret_from_entry >= 0.25, h, NULL)), 200) AS t_hit_up_25,
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

    -- best/worst points and when they happen within 7d
    (ARRAY_AGG(price_h_struct ORDER BY price DESC, h ASC LIMIT 1))[OFFSET(0)].price AS peak_price_7d,
    (ARRAY_AGG(price_h_struct ORDER BY price DESC, h ASC LIMIT 1))[OFFSET(0)].h     AS t_peak_h,
    (ARRAY_AGG(price_h_struct ORDER BY price ASC,  h ASC LIMIT 1))[OFFSET(0)].price AS trough_price_7d,
    (ARRAY_AGG(price_h_struct ORDER BY price ASC,  h ASC LIMIT 1))[OFFSET(0)].h     AS t_trough_h,

    -- max gain from entry & max drawdown vs running peak
    MAX(SAFE_DIVIDE(price, price0) - 1) AS max_gain_from_entry_7d,
    MIN(SAFE_DIVIDE(price, run_max_price) - 1) AS max_drawdown_7d,  -- negative

    -- realized volatility & hourly distribution (exclude h=0 via NULLs in seq)
    SQRT(SUM(POW(COALESCE(logret_1h, 0), 2))) * SQRT(24) AS rv_7d,
    STDDEV_SAMP(logret_1h) AS std_logret_1h_7d,
    AVG(logret_1h)         AS mean_logret_1h_7d,

    -- tail risk
    APPROX_QUANTILES(ABS(logret_1h), 100)[OFFSET(95)] AS p95_abs_logret_1h,

    -- directional persistence
    SAFE_DIVIDE(SUM(CASE WHEN logret_1h > 0 THEN 1 ELSE 0 END),
                NULLIF(SUM(CASE WHEN logret_1h IS NOT NULL THEN 1 ELSE 0 END), 0)) AS pct_pos_hours,

    -- rough Sharpe-like over the 7d path (mean/std * sqrt(24))
    SAFE_DIVIDE(AVG(logret_1h), NULLIF(STDDEV_SAMP(logret_1h), 0)) * SQRT(24) AS sharpe_like_7d,

    -- trend of log price: slope per hour & R^2
    SAFE_DIVIDE(
      COVAR_SAMP(LOG(NULLIF(price, 0)), CAST(h AS FLOAT64)),
      VAR_SAMP(CAST(h AS FLOAT64))
    ) AS slope_logprice_per_h,
    POW(CORR(CAST(h AS FLOAT64), LOG(NULLIF(price, 0))), 2) AS r2_logprice_trend,

    -- shape features
    AVG(cumret_from_entry) AS auc_cumret_avg,
    AVG(IF(h BETWEEN 0  AND 24,  cumret_from_entry, NULL)) AS avg_cumret_0_24h,
    AVG(IF(h BETWEEN 25 AND 72,  cumret_from_entry, NULL)) AS avg_cumret_25_72h,
    AVG(IF(h BETWEEN 73 AND 168, cumret_from_entry, NULL)) AS avg_cumret_73_168h
  FROM seq
  GROUP BY
    token_address, monitoring_session_id, first_acquired_timestamp,
    session_start, session_end, extended_start, extended_end
)

-- 6) Final projection
SELECT
  token_address,
  monitoring_session_id,
  first_acquired_timestamp,

  -- completeness
  n_points, max_h, has_full_7d,

  -- end states & daily checkpoints
  ret_24h, ret_48h, ret_72h, ret_96h, ret_120h, ret_144h, ret_168h,

  -- kinetics (hours to thresholds)
  t_hit_up_10, t_hit_up_15, t_hit_up_20, t_hit_up_25, t_hit_up_35, t_hit_up_40, t_hit_up_45, t_hit_up_50, t_hit_up_100, t_hit_up_200, t_hit_up_500,
  t_hit_dn_10, t_hit_dn_15, t_hit_dn_20, t_hit_dn_25, t_hit_dn_30, t_hit_dn_35, t_hit_dn_40, t_hit_dn_45, t_hit_dn_50,

  -- extremes
  max_gain_from_entry_7d,
  max_drawdown_7d,
  t_peak_h, t_trough_h,

  -- volatility & tail
  rv_7d,
  COALESCE(std_logret_1h_7d,  0) AS std_logret_1h_7d,
  COALESCE(mean_logret_1h_7d, 0) AS mean_logret_1h_7d,
  COALESCE(p95_abs_logret_1h, 0) AS p95_abs_logret_1h,
  pct_pos_hours,
  COALESCE(sharpe_like_7d, 0) AS sharpe_like_7d,

  -- trend & shape
  COALESCE(slope_logprice_per_h, 0) AS slope_logprice_per_h,
  COALESCE(r2_logprice_trend,   0) AS r2_logprice_trend,
  auc_cumret_avg, avg_cumret_0_24h, avg_cumret_25_72h, avg_cumret_73_168h

FROM per_series
ORDER BY token_address, first_acquired_timestamp
