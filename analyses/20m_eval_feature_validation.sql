-- Validation checks for rl_inference_features_next_open_v feature additions.
-- Expected result: all failing_rows values are 0, except sanity checks may be
-- interpreted statistically over a sufficiently large sample.

WITH base AS (
  SELECT
    *,
    ROW_NUMBER() OVER (
      PARTITION BY token_address
      ORDER BY price_timestamp
    ) AS token_rn,
    DENSE_RANK() OVER (ORDER BY price_timestamp) AS ts_rn
  FROM {{ ref('rl_inference_features_next_open_v') }}
),

universe_counts AS (
  SELECT
    price_timestamp,
    COUNTIF(active_universe) AS active_count,
    ANY_VALUE(CAST(univ_n_active AS INT64)) AS reported_active_count
  FROM base
  GROUP BY price_timestamp
),

rank_sanity AS (
  SELECT
    AVG(rel_rank_cumret_24h) AS avg_rank_cumret_24h
  FROM base
  WHERE active_universe
    AND rel_rank_cumret_24h IS NOT NULL
),

breadth_sanity AS (
  SELECT
    COUNTIF(univ_med_logret_24h > 0 AND univ_pct_pos_24h < 0.5) AS median_pos_but_less_than_half_positive
  FROM (
    SELECT DISTINCT
      price_timestamp,
      univ_med_logret_24h,
      univ_pct_pos_24h
    FROM base
    WHERE univ_n_active > 0
  )
)

SELECT
  'warmup_7d_trend_nulls' AS check_name,
  COUNTIF(
    token_rn < 168
    AND (
      trend_slope_7d IS NOT NULL
      OR trend_r2_7d IS NOT NULL
    )
  ) AS failing_rows,
  'trend_slope_7d and trend_r2_7d should be NULL before 168 token rows' AS detail
FROM base

UNION ALL

SELECT
  'warmup_30d_trend_nulls' AS check_name,
  COUNTIF(
    token_rn < 720
    AND (
      log_gap_from_30d_high IS NOT NULL
      OR log_gap_to_30d_low IS NOT NULL
      OR trend_slope_30d IS NOT NULL
      OR trend_r2_30d IS NOT NULL
    )
  ) AS failing_rows,
  '30d trend features should be NULL before 720 token rows' AS detail
FROM base

UNION ALL

SELECT
  'warmup_univ_vol_regime_30d_nulls' AS check_name,
  COUNTIF(ts_rn < 720 AND univ_med_rv_24h_z_30d IS NOT NULL) AS failing_rows,
  'univ_med_rv_24h_z_30d should be NULL before 720 distinct timestamps' AS detail
FROM base

UNION ALL

SELECT
  'warmup_btc_rv_pctile_90d_nulls' AS check_name,
  COUNTIF(ts_rn < 2160 AND btc_rv_24h_pctile_90d IS NOT NULL) AS failing_rows,
  'btc_rv_24h_pctile_90d should be NULL before 2160 distinct timestamps' AS detail
FROM base

UNION ALL

SELECT
  'universe_count_consistency' AS check_name,
  COUNTIF(active_count != reported_active_count) AS failing_rows,
  'COUNTIF(active_universe) should equal univ_n_active at every timestamp' AS detail
FROM universe_counts

UNION ALL

SELECT
  'relative_rank_center_sanity' AS check_name,
  CASE
    WHEN ABS(avg_rank_cumret_24h - 0.5) <= 0.02 THEN 0
    ELSE 1
  END AS failing_rows,
  CONCAT('Average rel_rank_cumret_24h is ', CAST(avg_rank_cumret_24h AS STRING), '; expected near 0.5') AS detail
FROM rank_sanity

UNION ALL

SELECT
  'breadth_median_direction_sanity' AS check_name,
  median_pos_but_less_than_half_positive AS failing_rows,
  'Rows where median 24h return > 0 but less than half the universe is positive' AS detail
FROM breadth_sanity
