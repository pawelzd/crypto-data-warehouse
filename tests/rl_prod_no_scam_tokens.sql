-- Causal born-bad evidence removes membership from that week forward. It does
-- not erase legitimate pre-evidence history from the candidate layer.
SELECT
  token_address,
  week_start,
  born_bad_patterns
FROM {{ ref('rl_prod_universe_membership_pit') }}
WHERE in_universe_pit
  AND is_born_bad
