{{ config(materialized='view') }}

-- depends_on: {{ ref('rl_prod_hourly_bars_v') }}
{{ rl_prod_asset_features_sql(
  'rl_prod_hourly_bars_v',
  var('trade_size_usd', 250.0)
) }}
