{{ config(severity='warn') }}

-- V2 has no minimum membership target. These are deliberately wide safety
-- bounds plus a feed/market-event alert for abrupt weekly contraction.
WITH weekly AS (
  SELECT
    week_start,
    COUNTIF(in_universe_pit) AS member_count
  FROM {{ ref('rl_prod_universe_membership_pit') }}
  GROUP BY week_start
),
lagged AS (
  SELECT
    *,
    LAG(member_count) OVER (ORDER BY week_start) AS previous_member_count
  FROM weekly
)
SELECT
  *,
  SAFE_DIVIDE(previous_member_count - member_count, previous_member_count) AS contraction
FROM lagged
WHERE member_count NOT BETWEEN 20 AND 150
   OR SAFE_DIVIDE(previous_member_count - member_count, previous_member_count) > 0.20
