select
    {{ dbt_utils.generate_surrogate_key(['token_address', 'price_timestamp']) }} as price_sk,
    
    token_address,
    price_timestamp,
    price_usd

from {{ ref('stg_solana_price_history') }}

{% if is_incremental() %}
  where price_timestamp > (select max(price_timestamp) from {{ this }})
{% endif %}
