SELECT
  token_address,
  week_start,
  in_universe_pit,
  entered_this_week,
  entry_quality_streak,
  low_mktcap_streak,
  bad_volume_streak,
  quality_fail_streak,
  is_born_bad,
  cap_boundary_evicted
FROM {{ ref('rl_prod_universe_membership_pit') }}
WHERE (entered_this_week AND (NOT meets_entry_rule OR entry_quality_streak < 2))
   OR (in_universe_pit AND low_mktcap_streak >= 2)
   OR (in_universe_pit AND bad_volume_streak >= 4)
   OR (in_universe_pit AND quality_fail_streak >= 4)
   OR (in_universe_pit AND is_born_bad)
   OR cap_boundary_evicted
   OR (in_universe_pit AND cap_seat_number > 150)
