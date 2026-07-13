SELECT
  token_address,
  week_start,
  in_universe_pit,
  entered_this_week,
  meets_entry_rule,
  low_mktcap_streak,
  bad_volume_streak
FROM {{ ref('rl_prod_universe_membership_pit') }}
WHERE (entered_this_week AND NOT meets_entry_rule)
   OR (in_universe_pit AND low_mktcap_streak >= 2)
   OR (in_universe_pit AND bad_volume_streak >= 4)
