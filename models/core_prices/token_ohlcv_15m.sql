{{ config(
  schema='core_prices',
  materialized='incremental',
  incremental_strategy='merge',
  unique_key=['token_address','price_timestamp','chain'],   
  on_schema_change='sync_all_columns',
  merge_update_columns=['close','high','open','low','volume','chain'] 
) }}

with source_data as (
  select *
  from {{ ref('birdeye_ohlcv_15m') }}
),

incoming as (
  select
    s.token_address,
    s.price_timestamp,
    s.close,
    s.high,
    s.open,
    s.low,
    s.volume,
    s.chain                               -- take chain from source
  from source_data s
)

select i.*
from incoming i
{% if is_incremental() %}
left join {{ this }} t
  on  t.token_address   = i.token_address
  and t.price_timestamp = i.price_timestamp
  and t.chain           = i.chain           -- include chain in the match
where t.token_address is null               -- only rows not already present
{% endif %}
