{{ config(
    materialized='table',
    tags=['gold'],
    schema='gold'
) }}
SELECT * 
FROM `gold.ml_token_prices_features7d_after_v`
where has_full_7d = 1 and ret_24h is not null