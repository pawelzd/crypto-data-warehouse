SELECT
  token_address,
  week_start,
  cap_boundary_evicted
FROM {{ ref('rl_prod_universe_membership_pit') }}
WHERE STARTS_WITH(rule_version, 'universe_dynamic_scaling_v2')
  AND cap_boundary_evicted

UNION ALL

SELECT
  '__weekly_cap__' AS token_address,
  week_start,
  FALSE AS cap_boundary_evicted
FROM {{ ref('rl_prod_universe_membership_pit') }}
WHERE STARTS_WITH(rule_version, 'universe_dynamic_scaling_v2')
GROUP BY week_start
HAVING COUNTIF(in_universe_pit) > 150
