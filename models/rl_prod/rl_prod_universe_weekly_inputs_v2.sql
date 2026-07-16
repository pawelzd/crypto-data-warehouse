{{ config(
  materialized='table',
  partition_by={
    'field': 'week_start',
    'data_type': 'date',
    'granularity': 'day'
  },
  cluster_by=['token_address']
) }}

-- Threshold-free, point-in-time weekly inputs for the v2 state builder.
-- Every range ends strictly before the Monday 00:00 UTC boundary. The single
-- shared YAML is consumed by the Python state machine, so this model exposes
-- measurements and causal evidence rather than duplicating policy values.
WITH
assets AS (
  SELECT
    token_address,
    price_timestamp,
    mktcap,
    dollar_vol_24h,
    dollar_amihud_24h,
    effective_spread_24h_bps
  FROM {{ ref('rl_prod_asset_features_history_v') }}
  WHERE token_address != 'btcusdt'
    AND has_168h
),

raw_observed_hours AS (
  SELECT
    token_address,
    TIMESTAMP_TRUNC(price_timestamp, HOUR) AS price_hour
  FROM {{ ref('token_ohlcv') }}
  WHERE chain = 'sol'
    AND token_address IS NOT NULL
    AND price_timestamp IS NOT NULL
    AND SAFE_CAST(close AS FLOAT64) > 0
  GROUP BY token_address, price_hour
),

bounds AS (
  SELECT
    DATE_TRUNC(MIN(DATE(price_timestamp)), WEEK(MONDAY)) AS first_week,
    DATE_TRUNC(MAX(DATE(price_timestamp)), WEEK(MONDAY)) AS last_week
  FROM assets
),

weeks AS (
  SELECT week_start
  FROM bounds,
  UNNEST(GENERATE_DATE_ARRAY(first_week, last_week, INTERVAL 7 DAY)) AS week_start
),

token_bounds AS (
  SELECT
    token_address,
    MIN(DATE(price_hour)) AS first_observed_date
  FROM raw_observed_hours
  WHERE token_address IN (SELECT DISTINCT token_address FROM assets)
  GROUP BY token_address
),

token_weeks AS (
  SELECT
    t.token_address,
    t.first_observed_date,
    w.week_start
  FROM token_bounds t
  INNER JOIN weeks w
    ON w.week_start >= DATE_TRUNC(t.first_observed_date, WEEK(MONDAY))
),

weekly_feature_metrics AS (
  SELECT
    a.token_address,
    w.week_start,
    COUNTIF(a.mktcap IS NOT NULL) AS feature_mktcap_hour_count_30d,
    APPROX_QUANTILES(a.mktcap, 100)[OFFSET(50)] AS median_mktcap_30d,
    APPROX_QUANTILES(a.dollar_vol_24h, 100)[OFFSET(50)] AS median_dollar_vol_30d,
    APPROX_QUANTILES(a.dollar_amihud_24h, 100)[OFFSET(50)] AS median_dollar_amihud_30d,
    APPROX_QUANTILES(a.effective_spread_24h_bps, 100)[OFFSET(90)] AS p90_effective_spread_30d_bps,
    MAX(a.price_timestamp) AS latest_feature_timestamp
  FROM weeks w
  INNER JOIN assets a
    ON a.price_timestamp >= TIMESTAMP_SUB(TIMESTAMP(w.week_start), INTERVAL 30 DAY)
   AND a.price_timestamp < TIMESTAMP(w.week_start)
  GROUP BY a.token_address, w.week_start
),

observed_week_hours AS (
  SELECT
    r.token_address,
    w.week_start,
    r.price_hour,
    LAG(r.price_hour) OVER (
      PARTITION BY r.token_address, w.week_start
      ORDER BY r.price_hour
    ) AS previous_price_hour
  FROM weeks w
  INNER JOIN raw_observed_hours r
    ON r.price_hour >= TIMESTAMP_SUB(TIMESTAMP(w.week_start), INTERVAL 30 DAY)
   AND r.price_hour < TIMESTAMP(w.week_start)
),

weekly_continuity AS (
  SELECT
    token_address,
    week_start,
    COUNT(*) AS observed_hour_count_30d,
    GREATEST(
      TIMESTAMP_DIFF(
        MIN(price_hour),
        TIMESTAMP_SUB(TIMESTAMP(week_start), INTERVAL 30 DAY),
        HOUR
      ),
      TIMESTAMP_DIFF(TIMESTAMP(week_start), MAX(price_hour), HOUR) - 1,
      COALESCE(
        MAX(
          IF(
            previous_price_hour IS NULL,
            NULL,
            TIMESTAMP_DIFF(price_hour, previous_price_hour, HOUR) - 1
          )
        ),
        0
      )
    ) AS longest_gap_hours_30d,
    MAX(price_hour) AS latest_observed_timestamp
  FROM observed_week_hours
  GROUP BY token_address, week_start
),

scam_evidence_by_pattern AS (
  SELECT
    e.token_address,
    w.week_start,
    e.scam_pattern,
    COUNT(DISTINCT e.window_start) AS distinct_windows,
    DATE_DIFF(MAX(e.window_start), MIN(e.window_start), DAY) AS evidence_span_days,
    MAX(e.window_end) AS latest_window_end
  FROM weeks w
  INNER JOIN {{ source('scam_detection_artifacts', 'scam_full_history_detections') }} e
    ON e.chain = 'sol'
   AND e.window_end < w.week_start
  GROUP BY e.token_address, w.week_start, e.scam_pattern
),

scam_evidence_weekly AS (
  SELECT
    token_address,
    week_start,
    TO_JSON_STRING(
      ARRAY_AGG(
        STRUCT(
          scam_pattern,
          distinct_windows,
          evidence_span_days,
          latest_window_end
        )
        ORDER BY scam_pattern
      )
    ) AS scam_evidence_json,
    MAX(latest_window_end) AS latest_scam_window_end
  FROM scam_evidence_by_pattern
  GROUP BY token_address, week_start
),

manual_scam AS (
  SELECT DISTINCT token_address
  FROM {{ ref('scam_manual') }}
  WHERE chain = 'sol'
)

SELECT
  t.token_address,
  t.first_observed_date,
  t.week_start,
  COALESCE(c.observed_hour_count_30d, 0) AS observed_hour_count_30d,
  COALESCE(c.longest_gap_hours_30d, 720) AS longest_gap_hours_30d,
  COALESCE(f.feature_mktcap_hour_count_30d, 0) AS feature_mktcap_hour_count_30d,
  f.median_mktcap_30d,
  f.median_dollar_vol_30d,
  f.median_dollar_amihud_30d,
  f.p90_effective_spread_30d_bps,
  s.scam_evidence_json,
  m.token_address IS NOT NULL AS is_manual_scam,
  c.latest_observed_timestamp,
  f.latest_feature_timestamp,
  s.latest_scam_window_end
FROM token_weeks t
LEFT JOIN weekly_continuity c
  USING (token_address, week_start)
LEFT JOIN weekly_feature_metrics f
  USING (token_address, week_start)
LEFT JOIN scam_evidence_weekly s
  USING (token_address, week_start)
LEFT JOIN manual_scam m
  USING (token_address)
