{{ config(materialized='view') }}

-- depends_on: {{ ref('token_ohlcv') }}
-- depends_on: {{ ref('birdeye_market_data') }}
{{ rl_prod_hourly_bars_sql(var('rl_prod_history_hours', 960)) }}
