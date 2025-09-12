
WITH base AS (
  -- Keep the 0..168h window after acquisition, one row per hour
  SELECT
    token_address,
    type,
    first_acquired_timestamp,
    TIMESTAMP_TRUNC(price_timestamp, HOUR) AS ts_hour,
    CAST(price_usd AS FLOAT64) AS price,
    CAST(price_usd_on_acquisition AS FLOAT64) AS price0,
    CAST(hours_change_from_acquisition AS INT64) AS h
  FROM {{ ref('ml_tokens_prices_filtered_7daysafter') }}
  WHERE price_usd IS NOT NULL
    AND hours_change_from_acquisition BETWEEN 0 AND 168
),
seq AS (
  -- Per series time-ordered sequence and basic path metrics
  SELECT
    b.*,
    SAFE.LOG(SAFE_DIVIDE(b.price, LAG(b.price) OVER w)) AS logret_1h,
    SAFE_DIVIDE(b.price, MAX(IF(b.h = 0, b.price, NULL)) OVER (PARTITION BY b.token_address, b.first_acquired_timestamp, b.type)) - 1
      AS cumret_from_entry,
    -- running peak for drawdowns
    MAX(b.price) OVER (PARTITION BY b.token_address, b.first_acquired_timestamp, b.type
                       ORDER BY b.h ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS run_max_price,
    -- handy structs to capture argmax/argmin later
    STRUCT(b.price AS price, b.h AS h) AS price_h_struct
  FROM base b
  WINDOW w AS (PARTITION BY b.token_address, b.first_acquired_timestamp, b.type ORDER BY b.h)
),
per_series AS (
  SELECT
    token_address,
    type,
    first_acquired_timestamp,

    -- completeness
    COUNTIF(h IS NOT NULL) AS n_points,
    MAX(h) AS max_h,
    CASE WHEN COUNTIF(h IS NOT NULL) >= 169 AND MAX(h) = 168 AND MIN(h) = 0 THEN 1 ELSE 0 END AS has_full_7d,

    -- end states & daily checkpoints (shape features; returns from entry)
    ANY_VALUE(price0) AS price0,
    MAX(IF(h = 24,  SAFE_DIVIDE(price, price0) - 1, NULL)) AS ret_24h,
    MAX(IF(h = 48,  SAFE_DIVIDE(price, price0) - 1, NULL)) AS ret_48h,
    MAX(IF(h = 72,  SAFE_DIVIDE(price, price0) - 1, NULL)) AS ret_72h,
    MAX(IF(h = 96,  SAFE_DIVIDE(price, price0) - 1, NULL)) AS ret_96h,
    MAX(IF(h = 120, SAFE_DIVIDE(price, price0) - 1, NULL)) AS ret_120h,
    MAX(IF(h = 144, SAFE_DIVIDE(price, price0) - 1, NULL)) AS ret_144h,
    MAX(IF(h = 168, SAFE_DIVIDE(price, price0) - 1, NULL)) AS ret_168h, -- "final" 7d return

    -- kinetics: time to hit thresholds (hours; NULL if never hit)
    COALESCE(MIN(IF(cumret_from_entry >= 0.10, h, NULL)), 200) AS t_hit_up_10,
    COALESCE(MIN(IF(cumret_from_entry >= 0.20, h, NULL)), 200) AS t_hit_up_25,
    COALESCE(MIN(IF(cumret_from_entry >= 0.25, h, NULL)), 200) AS t_hit_up_35,
    COALESCE(MIN(IF(cumret_from_entry >= 0.50, h, NULL)), 200) AS t_hit_up_50,
    COALESCE(MIN(IF(cumret_from_entry >= 1.00, h, NULL)), 200) AS t_hit_up_100,
    COALESCE(MIN(IF(cumret_from_entry >= 2.00, h, NULL)), 200) AS t_hit_up_200,
    COALESCE(MIN(IF(cumret_from_entry >= 5.00, h, NULL)), 200) AS t_hit_up_500,
    COALESCE(MIN(IF(cumret_from_entry <= -0.10, h, NULL)), 200) AS t_hit_dn_20,
    COALESCE(MIN(IF(cumret_from_entry <= -0.20, h, NULL)), 200) AS t_hit_dn_35,
    COALESCE(MIN(IF(cumret_from_entry <= -0.25, h, NULL)), 200) AS t_hit_dn_25,
    COALESCE(MIN(IF(cumret_from_entry <= -0.50, h, NULL)), 200) AS t_hit_dn_50,

    -- best/worst points and when they happen
    -- argmax/argmin via ARRAY_AGG on structs
    (ARRAY_AGG(price_h_struct ORDER BY price DESC, h ASC LIMIT 1))[OFFSET(0)].price AS peak_price_7d,
    (ARRAY_AGG(price_h_struct ORDER BY price DESC, h ASC LIMIT 1))[OFFSET(0)].h     AS t_peak_h,
    (ARRAY_AGG(price_h_struct ORDER BY price ASC,  h ASC LIMIT 1))[OFFSET(0)].price AS trough_price_7d,
    (ARRAY_AGG(price_h_struct ORDER BY price ASC,  h ASC LIMIT 1))[OFFSET(0)].h     AS t_trough_h,

    -- max gain from entry & max drawdown from running peak
    MAX(SAFE_DIVIDE(price, price0) - 1) AS max_gain_from_entry_7d,
    MIN(SAFE_DIVIDE(price, run_max_price) - 1) AS max_drawdown_7d,  -- negative number

    -- realized volatility and dispersion of hourly returns in the window (exclude h=0)
    SQRT(SUM(POW(COALESCE(logret_1h, 0), 2))) * SQRT(24) AS rv_7d,
    STDDEV_SAMP(logret_1h) AS std_logret_1h_7d,
    AVG(CASE WHEN logret_1h IS NULL THEN NULL ELSE (logret_1h) END) AS mean_logret_1h_7d,
    -- tail risk
    APPROX_QUANTILES(IF(logret_1h IS NULL, NULL, ABS(logret_1h)), 100)[OFFSET(95)] AS p95_abs_logret_1h,


    -- proportion of positive hours (directional persistence)
    SAFE_DIVIDE(SUM(CASE WHEN logret_1h > 0 THEN 1 ELSE 0 END), NULLIF(SUM(CASE WHEN logret_1h IS NOT NULL THEN 1 ELSE 0 END), 0)) AS pct_pos_hours,

    -- rough Sharpe-like over the 7d path (mean/std * sqrt(24))
    SAFE_DIVIDE(AVG(logret_1h), NULLIF(STDDEV_SAMP(logret_1h), 0)) * SQRT(24) AS sharpe_like_7d,

    -- trend of log price: slope per hour and goodness of fit
SAFE_DIVIDE(
  COVAR_SAMP(SAFE.LOG(NULLIF(price, 0)), CAST(h AS FLOAT64)),
  VAR_SAMP(CAST(h AS FLOAT64))
) AS slope_logprice_per_h,
POW(CORR(CAST(h AS FLOAT64), SAFE.LOG(NULLIF(price, 0))), 2) AS r2_logprice_trend,
    -- average path "area under curve" for cumulative return (shape proxy)
    AVG(cumret_from_entry) AS auc_cumret_avg,

    -- early vs late move (front-loaded vs back-loaded)
    AVG(IF(h BETWEEN 0 AND 24, cumret_from_entry, NULL))  AS avg_cumret_0_24h,
    AVG(IF(h BETWEEN 25 AND 72, cumret_from_entry, NULL)) AS avg_cumret_25_72h,
    AVG(IF(h BETWEEN 73 AND 168, cumret_from_entry, NULL)) AS avg_cumret_73_168h
  FROM seq
  GROUP BY token_address, type, first_acquired_timestamp
)

SELECT
  token_address,
  type,
  first_acquired_timestamp,

  -- completeness
  n_points, max_h, has_full_7d,

  -- end state & daily checkpoints (feature vector for clustering)
  ret_24h, ret_48h, ret_72h, ret_96h, ret_120h, ret_144h, ret_168h,

  -- kinetics
  t_hit_up_10, t_hit_up_25, t_hit_up_35, t_hit_up_50, t_hit_up_100, t_hit_up_200, t_hit_up_500
  t_hit_dn_10, t_hit_dn_25, t_hit_dn_35, t_hit_dn_50,

  -- extremes
  max_gain_from_entry_7d,
  max_drawdown_7d,
  t_peak_h, t_trough_h,

  -- volatility & tail
  rv_7d, COALESCE(std_logret_1h_7d, 0) as std_logret_1h_7d, COALESCE(mean_logret_1h_7d, 0) as mean_logret_1h_7d, COALESCE(p95_abs_logret_1h, 0) as p95_abs_logret_1h,
  pct_pos_hours, COALESCE(sharpe_like_7d, 0) as sharpe_like_7d,

  -- trend & shape
  COALESCE(slope_logprice_per_h, 0) as slope_logprice_per_h, 
  COALESCE(r2_logprice_trend, 0) as r2_logprice_trend,
  auc_cumret_avg, avg_cumret_0_24h, avg_cumret_25_72h, avg_cumret_73_168h

FROM per_series