{{ config(severity='warn') }}

WITH weekly AS (
  SELECT
    week_start,
    COUNTIF(in_universe_pit) AS member_count,
    COUNTIF(entered_this_week) AS entries,
    COUNTIF(exited_this_week) AS exits
  FROM {{ ref('rl_prod_universe_membership_pit') }}
  GROUP BY week_start
),
with_previous AS (
  SELECT
    *,
    LAG(member_count) OVER (ORDER BY week_start) AS previous_member_count
  FROM weekly
)
SELECT
  *,
  SAFE_DIVIDE(entries + exits, previous_member_count) AS turnover
FROM with_previous
WHERE previous_member_count > 0
  AND week_start >= DATE '2024-01-01'
  AND SAFE_DIVIDE(entries + exits, previous_member_count) > 0.10
