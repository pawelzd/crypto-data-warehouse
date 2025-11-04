{{config(
    materialized='view',
)
}}
SELECT 
    token_chain_id AS token_address,
    price_timestamp,
    price_usd,
    volume,
    mktcap,
    chain
FROM {{ref('cv_prep_ema21_ema50')}} c