{{ config(materialized='view') }}

-- depends_on: {{ ref('rl_prod_asset_features_v') }}
-- include_edge=true: this is the LIVE serving view — keep the freshest bar so the
-- decision service acts on the latest closed bar (the runner fills the missing
-- return-mark with the live price). history_v / training views omit it.
-- retain_recent_member_weeks=2: LIVE view only. Keep emitting a token for 2 weeks
-- after it leaves the universe, so an open position in it can still be CLOSED --
-- the runner steps only what this view emits, so without it a held token that
-- loses membership (which can happen any Monday) is never stepped and strands in
-- the ledger. An episode caps at 216 bars (~9 days), so 2 weeks covers anything
-- this system can hold. History/training views leave it at 0 and compile
-- byte-identical. Entries are unaffected: paper_trade still gates BUY on
-- in_universe_pit per bar.
-- NB comments must stay OUTSIDE the call -- a `--` comment between arguments is
-- not valid inside a Jinja expression and fails dbt compilation.
{{ rl_prod_inference_features_sql(
  'rl_prod_asset_features_v',
  var('rl_prod_output_hours', 48),
  var('rl_prod_min_mktcap', 0),
  true,
  var('rl_prod_minimum_member_coverage', 0.90),
  include_edge=true,
  retain_recent_member_weeks=2
) }}
