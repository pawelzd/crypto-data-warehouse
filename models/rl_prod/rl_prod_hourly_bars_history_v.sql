{{ config(materialized='view') }}

-- Full source history for parity checks; never use this relation in the
-- hourly inference path.
-- depends_on: {{ ref('token_ohlcv') }}
-- depends_on: {{ ref('birdeye_market_data') }}
{{ rl_prod_hourly_bars_sql(none) }}
