{{ config(materialized='view') }}

-- depends_on: {{ ref('rl_prod_asset_features_v') }}
-- include_edge=true: this is the LIVE serving view — keep the freshest bar so the
-- decision service acts on the latest closed bar (the runner fills the missing
-- return-mark with the live price). history_v / training views omit it.
{{ rl_prod_inference_features_sql(
  'rl_prod_asset_features_v',
  var('rl_prod_output_hours', 48),
  var('rl_prod_min_mktcap', 0),
  true,
  var('rl_prod_minimum_member_coverage', 0.90),
  include_edge=true,
  -- LIVE view only: keep a token that left the universe in the last 2 weeks, so an
  -- open position in it can still be closed. An episode caps at 216 bars (9 days),
  -- so 2 weeks covers any position this system can hold. History/training views
  -- leave this at 0 and compile unchanged.
  retain_recent_member_weeks=2
) }}
