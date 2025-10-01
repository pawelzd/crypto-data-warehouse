{{ config(
    schema='gold_ml_coins_mon_72_prod',
    materialized='view'
) }}
with source_data as (
    select * from {{ ref('fct__token_prices') }}
),

trading_activity as (
    select * from {{ ref('fct__token_trading_activity') }}
)


select
    s.token_address AS address,
    s.price_timestamp AS datetime,
    s.price_usd AS price, 
    ABS(t.delta_buy_bal_1h)+ ABS(t.delta_sell_bal_1h) AS volume,
    
from source_data as s
left join trading_activity as t
    on s.token_address = t.token_address
   and s.price_timestamp = t.first_acquired_timestamp

where s.price_usd is not null
  and s.token_address is not null
