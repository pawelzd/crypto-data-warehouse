{{
  config(
    materialized='incremental',
    unique_key='token_address',
    on_schema_change='sync_all_columns'
  )
}}

with source_data as (
  select * from {{ ref('stg_token_market_metadata') }}
)

select
  {{ dbt_utils.generate_surrogate_key(['s.token_address']) }} as token_metadata_sk,
  s.token_address,
  s.price_usd,
  s.liquidity,
  s.total_supply,
  s.circulating_supply,
  s.fdv_usd,
  s.market_cap_usd,
  s.is_scaled_ui_token,
  s.multiplier,
from source_data as s

{% if is_incremental() %}
-- Merge behavior handled by unique_key; no additional filter required.
{% endif %}

