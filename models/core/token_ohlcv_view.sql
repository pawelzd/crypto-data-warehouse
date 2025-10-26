{{
    config(
    schema='core',       
    materialized='view'
) }}

select 
    *,
    (token_address || '_' || chain) as token_chain_id
from {{ref('token_ohlcv')}}