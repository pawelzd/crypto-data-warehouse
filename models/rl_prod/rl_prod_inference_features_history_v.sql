{{ config(materialized='view') }}

-- Full-history contract for parity checks against the legacy feature set.
-- depends_on: {{ ref('rl_prod_asset_features_history_v') }}
{{ rl_prod_inference_features_sql(
  'rl_prod_asset_features_history_v',
  none,
  var('rl_prod_min_mktcap', 0),
  false
) }}
