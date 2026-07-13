{{ config(
  schema='core',
  materialized='incremental',
  full_refresh=false,
  incremental_strategy='merge',
  unique_key=['token_address','price_timestamp','chain'],
  partition_by={
    'field': 'price_timestamp',
    'data_type': 'timestamp',
    'granularity': 'day'
  },
  cluster_by=['chain', 'token_address'],
  incremental_predicates=[
    "DBT_INTERNAL_DEST.price_timestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 7 DAY)"
  ],
  on_schema_change='sync_all_columns',
  merge_update_columns=['close','high','open','low','volume','chain'] 
) }}

with source_data as (
  select *
  from {{ ref('birdeye_ohlcv') }}
  where token_address not in (
    select token_address
    from {{ ref('birdeye_ohlcv') }}
    group by token_address
    having abs(avg(close) - 1) <= 0.05
  )
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
