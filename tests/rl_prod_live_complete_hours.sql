WITH expected AS (
  SELECT
    week_start,
    COUNTIF(in_universe_pit) AS expected_member_count
  FROM {{ ref('rl_prod_universe_membership_pit') }}
  GROUP BY week_start
)
SELECT
  v.price_timestamp,
  MIN(v.univ_n_active) AS actual_members,
  MAX(e.expected_member_count) AS expected_members
FROM {{ ref('rl_prod_inference_features_v') }} v
INNER JOIN expected e
  ON e.week_start = DATE_TRUNC(DATE(v.price_timestamp), WEEK(MONDAY))
GROUP BY v.price_timestamp
HAVING actual_members < expected_members * 0.90
