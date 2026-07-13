{{ config(severity='warn') }}

-- The band is a distribution audit, not permission to fabricate membership.
WITH weekly AS (
  SELECT
    week_start,
    COUNTIF(in_universe_pit) AS member_count
  FROM {{ ref('rl_prod_universe_membership_pit') }}
  WHERE week_start >= DATE '2024-01-01'
  GROUP BY week_start
)
SELECT *
FROM weekly
WHERE member_count NOT BETWEEN 120 AND 250
   OR (week_start >= DATE '2026-06-01' AND member_count < 100)
