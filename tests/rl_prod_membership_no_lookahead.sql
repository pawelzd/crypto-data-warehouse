SELECT
  token_address,
  week_start,
  first_observed_date
FROM {{ ref('rl_prod_universe_membership_pit') }}
WHERE in_universe_pit
  AND week_start < first_observed_date
