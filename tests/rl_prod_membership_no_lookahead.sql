SELECT
  token_address,
  week_start,
  first_observed_date,
  latest_observed_timestamp,
  latest_feature_timestamp,
  latest_scam_window_end
FROM {{ ref('rl_prod_universe_membership_pit') }}
WHERE week_start < DATE_TRUNC(first_observed_date, WEEK(MONDAY))
   OR latest_observed_timestamp >= TIMESTAMP(week_start)
   OR latest_feature_timestamp >= TIMESTAMP(week_start)
   OR latest_scam_window_end >= week_start
