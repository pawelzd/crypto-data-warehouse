{{ config(
  materialized='table',
  partition_by={
    'field': 'week_start',
    'data_type': 'date',
    'granularity': 'day'
  },
  cluster_by=['token_address']
) }}

-- Stateful weekly PIT membership. Rebuild this table after source OHLCV,
-- market metadata, or hardened scam exclusions change.
WITH RECURSIVE
assets AS (
  SELECT
    token_address,
    price_timestamp,
    mktcap,
    dollar_vol_24h
  FROM {{ ref('rl_prod_asset_features_history_v') }}
  WHERE token_address != 'btcusdt'
    AND has_168h
),

scam_tokens AS (
  SELECT DISTINCT token_address
  FROM {{ ref('scam_h_union') }}
  WHERE chain = 'sol'
),

token_bounds AS (
  SELECT
    token_address,
    MIN(DATE(price_timestamp)) AS first_observed_date
  FROM assets a
  WHERE NOT EXISTS (
    SELECT 1
    FROM scam_tokens s
    WHERE s.token_address = a.token_address
  )
  GROUP BY token_address
),

bounds AS (
  SELECT
    DATE_TRUNC(MIN(DATE(price_timestamp)), WEEK(MONDAY)) AS first_week,
    DATE_TRUNC(MAX(DATE(price_timestamp)), WEEK(MONDAY)) AS last_week
  FROM assets
),

weeks AS (
  SELECT
    week_start,
    ROW_NUMBER() OVER (ORDER BY week_start) AS global_week_number
  FROM bounds,
  UNNEST(GENERATE_DATE_ARRAY(first_week, last_week, INTERVAL 7 DAY)) AS week_start
),

weekly_candidate_metrics AS (
  SELECT
    a.token_address,
    w.week_start,
    COUNT(*) AS trailing_bar_count,
    APPROX_QUANTILES(a.mktcap, 100)[OFFSET(50)] AS median_mktcap_30d,
    APPROX_QUANTILES(a.dollar_vol_24h, 100)[OFFSET(50)] AS median_dollar_vol_30d
  FROM weeks w
  INNER JOIN assets a
    ON a.price_timestamp >= TIMESTAMP_SUB(TIMESTAMP(w.week_start), INTERVAL 30 DAY)
   AND a.price_timestamp < TIMESTAMP(w.week_start)
  WHERE NOT EXISTS (
    SELECT 1
    FROM scam_tokens s
    WHERE s.token_address = a.token_address
  )
  GROUP BY a.token_address, w.week_start
),

ranked_candidates AS (
  SELECT
    m.*,
    CASE
      WHEN trailing_bar_count >= 500
        AND median_dollar_vol_30d IS NOT NULL
      THEN RANK() OVER (
        PARTITION BY week_start
        ORDER BY
          IF(trailing_bar_count >= 500, median_dollar_vol_30d, NULL) DESC NULLS LAST,
          token_address
      )
    END AS dollar_volume_rank_30d
  FROM weekly_candidate_metrics m
),

eligible_token_weeks AS (
  SELECT
    t.token_address,
    t.first_observed_date,
    w.week_start,
    ROW_NUMBER() OVER (
      PARTITION BY t.token_address ORDER BY w.week_start
    ) AS token_week_number
  FROM token_bounds t
  INNER JOIN weeks w
    ON w.week_start >= t.first_observed_date
),

weekly_inputs AS (
  SELECT
    e.token_address,
    e.first_observed_date,
    e.week_start,
    e.token_week_number,
    COALESCE(r.trailing_bar_count, 0) AS trailing_bar_count,
    r.median_mktcap_30d,
    r.median_dollar_vol_30d,
    r.dollar_volume_rank_30d,
    COALESCE(r.median_mktcap_30d < 5000000, TRUE) AS below_exit_mktcap,
    COALESCE(r.dollar_volume_rank_30d > 250, TRUE) AS below_exit_volume,
    COALESCE(
      r.trailing_bar_count >= 500
      AND r.median_mktcap_30d >= 20000000
      AND r.dollar_volume_rank_30d <= 120,
      FALSE
    ) AS meets_entry_rule
  FROM eligible_token_weeks e
  LEFT JOIN ranked_candidates r
    USING (token_address, week_start)
),

membership_state AS (
  SELECT
    i.*,
    CAST(IF(i.below_exit_mktcap, 1, 0) AS INT64) AS low_mktcap_streak,
    CAST(IF(i.below_exit_volume, 1, 0) AS INT64) AS bad_volume_streak,
    i.meets_entry_rule AS in_universe_pit
  FROM weekly_inputs i
  WHERE token_week_number = 1

  UNION ALL

  SELECT
    i.*,
    CAST(
      IF(i.below_exit_mktcap, p.low_mktcap_streak + 1, 0)
      AS INT64
    ) AS low_mktcap_streak,
    CAST(
      IF(i.below_exit_volume, p.bad_volume_streak + 1, 0)
      AS INT64
    ) AS bad_volume_streak,
    CASE
      WHEN p.in_universe_pit THEN NOT (
        (i.below_exit_mktcap AND p.low_mktcap_streak + 1 >= 2)
        OR (i.below_exit_volume AND p.bad_volume_streak + 1 >= 4)
      )
      ELSE i.meets_entry_rule
    END AS in_universe_pit
  FROM membership_state p
  INNER JOIN weekly_inputs i
    ON i.token_address = p.token_address
   AND i.token_week_number = p.token_week_number + 1
),

with_events AS (
  SELECT
    s.*,
    LAG(in_universe_pit, 1, FALSE) OVER (
      PARTITION BY token_address ORDER BY week_start
    ) AS previous_in_universe
  FROM membership_state s
)

SELECT
  token_address,
  first_observed_date,
  week_start,
  trailing_bar_count,
  median_mktcap_30d,
  median_dollar_vol_30d,
  dollar_volume_rank_30d,
  low_mktcap_streak,
  bad_volume_streak,
  meets_entry_rule,
  in_universe_pit,
  in_universe_pit AND NOT previous_in_universe AS entered_this_week,
  previous_in_universe AND NOT in_universe_pit AS exited_this_week,
  CURRENT_TIMESTAMP() AS generated_at
FROM with_events
