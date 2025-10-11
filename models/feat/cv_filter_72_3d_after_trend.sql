-- Predict next-24h trend labels on 1h prices
-- Output: one row per acquisition timestamp with forward stats + 3-class label

{{ config(
    schema='feat',
    materialized='table'
) }}
-- 1) Hourly prices from extended windows (keep session metadata)
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
  FROM {{ ref('cv_filter_prep_72_ext_windows') }}
  WHERE price_usd IS NOT NULL
  GROUP BY
    token_address, monitoring_session_id, session_start, session_end,
    extended_start, extended_end, in_pre_extension, in_core_monitoring, in_post_extension, ts_hour
),

-- 2) Candidate acquisition timestamps (ensure we can see 24h ahead)
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
    AND TIMESTAMP_ADD(ts_hour, INTERVAL 24 HOUR) <= extended_end
),

-- 3) Build the 0..24h forward window per acquisition
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
    CAST(TIMESTAMP_DIFF(h.ts_hour, a.first_acquired_timestamp, HOUR) AS INT64) AS h
  FROM acquisitions a
  JOIN hourly h
    ON h.token_address        = a.token_address
   AND h.monitoring_session_id = a.monitoring_session_id
   AND h.ts_hour BETWEEN a.first_acquired_timestamp
                     AND TIMESTAMP_ADD(a.first_acquired_timestamp, INTERVAL 24 HOUR)
),

-- 4) Forward stats per hour within each acquisition
seq AS (
  SELECT
    b.*,
    -- entry price (h=0)
    MAX(IF(b.h = 0, b.price, NULL)) OVER (PARTITION BY b.token_address, b.monitoring_session_id, b.first_acquired_timestamp) AS price0,

    -- simple per-hour log return (safe)
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

    -- cumulative return from entry
    SAFE_DIVIDE(b.price, MAX(IF(b.h = 0, b.price, NULL)) OVER (PARTITION BY b.token_address, b.monitoring_session_id, b.first_acquired_timestamp)) - 1
      AS cumret_from_entry,

    -- index for OLS helpers
    ROW_NUMBER() OVER (
      PARTITION BY b.token_address, b.monitoring_session_id, b.first_acquired_timestamp
      ORDER BY b.h
    ) AS idx,
    SAFE.LOG(b.price) AS logp
  FROM base b
),

-- 5) Forward-looking metrics over [h=0 .. h=24] computed at h=0
fwd AS (
  SELECT
    s.*,
    -- arrays over forward 24h (including current)
    ARRAY_AGG(price) OVER wf24 AS arr_price_24,
    -- price at t+24h
    LEAD(price, 24) OVER w AS price_fwd_24,

    -- Forward-window OLS slope & R^2 on log price vs time index
    (
      (SUM(idx*logp) OVER wf24 - (SUM(idx) OVER wf24)*(SUM(logp) OVER wf24)/25.0)
      / NULLIF( (SUM(idx*idx) OVER wf24 - (SUM(idx) OVER wf24)*(SUM(idx) OVER wf24)/25.0), 0)
    ) AS slope_fwd_24,
    POW(
      (SUM(idx*logp) OVER wf24 - (SUM(idx) OVER wf24)*(SUM(logp) OVER wf24)/25.0),
      2
    ) / NULLIF(
      (SUM(idx*idx) OVER wf24 - (SUM(idx) OVER wf24)*(SUM(idx) OVER wf24)/25.0)
      * (SUM(logp*logp) OVER wf24 - (SUM(logp) OVER wf24)*(SUM(logp) OVER wf24)/25.0),
      0
    ) AS r2_fwd_24
  FROM seq s
  WINDOW
    w    AS (PARTITION BY token_address, monitoring_session_id, first_acquired_timestamp ORDER BY h),
    wf24 AS (PARTITION BY token_address, monitoring_session_id, first_acquired_timestamp
             ORDER BY h ROWS BETWEEN CURRENT ROW AND 24 FOLLOWING)
),

-- 6) Collapse to one row per acquisition (take values at h=0)
per_acq AS (
  SELECT
    token_address,
    monitoring_session_id,
    first_acquired_timestamp,
    session_start,
    session_end,
    extended_start,
    extended_end,

    -- completeness
    COUNT(*) AS n_points_0_24h,
    MAX(h)   AS max_h_0_24h,
    CASE WHEN COUNT(*) = 25 AND MIN(h) = 0 AND MAX(h) = 24 THEN 1 ELSE 0 END AS has_full_24h,

    -- entry price
    ANY_VALUE(price0) AS price0,

    -- forward end state at +24h (log return)
    ANY_VALUE(
      CASE WHEN price0 > 0 AND price_fwd_24 > 0 THEN LOG(price_fwd_24/price0) ELSE NULL END
    ) FILTER (WHERE h = 0) AS fwd_logret_24,

    -- forward path cleanliness
    ANY_VALUE(r2_fwd_24) FILTER (WHERE h = 0) AS r2_fwd_24,

    -- MFE/MAE over next 24h using hourly price (no H/L available here)
    ANY_VALUE(
      CASE WHEN price0 > 0 THEN LOG( (SELECT MAX(p) FROM UNNEST(arr_price_24) p) / price0 ) ELSE NULL END
    ) FILTER (WHERE h = 0) AS mfe_24,
    ANY_VALUE(
      CASE WHEN price0 > 0 THEN LOG( (SELECT MIN(p) FROM UNNEST(arr_price_24) p) / price0 ) ELSE NULL END
    ) FILTER (WHERE h = 0) AS mae_24
  FROM fwd
  GROUP BY
    token_address, monitoring_session_id, first_acquired_timestamp,
    session_start, session_end, extended_start, extended_end
),

-- 7) Apply labeling rules (tunable thresholds)
rules AS (
  SELECT
    *,
    0.015  AS thr_ret,    -- +1.5% forward move (log ~ 1.5%)
    0.50   AS thr_r2,     -- path cleanliness
    -0.006 AS thr_mae     -- allow up to -0.6% adverse move
  FROM per_acq
)

-- 8) Final labels (3-class: +1 up, -1 down, 0 none)
SELECT
  token_address,
  monitoring_session_id,
  first_acquired_timestamp,
  n_points_0_24h,
  max_h_0_24h,
  has_full_24h,
  price0,
  fwd_logret_24,
  r2_fwd_24,
  mfe_24,
  mae_24,
  CASE
    WHEN fwd_logret_24 >=  thr_ret AND r2_fwd_24 >= thr_r2 AND mae_24 >= thr_mae THEN  1
    WHEN fwd_logret_24 <= -thr_ret AND r2_fwd_24 >= thr_r2 AND mfe_24 <= -thr_mae THEN -1
    ELSE 0
  END AS label_k24
FROM rules