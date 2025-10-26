{{ config(
    materialized = 'incremental',
    unique_key = 'token_chain_id'   
) }}

SELECT
    *,
    (token_address || '_' || chain) AS token_chain_id
FROM {{ ref('birdeye_market_data') }}
WHERE token_address IS NOT NULL
  AND market_cap_usd IS NOT NULL
  AND total_supply IS NOT NULL

{% if is_incremental() %}
  -- this part only runs when dbt run incrementally
  AND (token_address || '_' || chain) NOT IN (
    SELECT token_chain_id FROM {{ this }}
  )
{% endif %}
