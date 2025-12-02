{{
    config(
    schema='core',       
    materialized='view'
) }}

select 
    *,
    (token_address || '_' || chain) as token_chain_id
from {{ref('token_ohlcv')}} tocv
where not exists (select 1 from {{ref('token_missing_data_h')}} tmd where tocv.token_address = tmd.token_address and tocv.chain = tmd.chain)
