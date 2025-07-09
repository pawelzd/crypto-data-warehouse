with source_data as (

    select * from {{ ref('stg_solana_price_history') }}

)

select
    {{ dbt_utils.generate_surrogate_key(['token_address', 'price_timestamp']) }} as price_sk,
    token_address,
    price_timestamp,
    price_usd
from source_data

{% if is_incremental() %}

-- Only insert records that are new or more recent than the latest record for that specific token
where not exists (
    select 1
    from {{ this }}
    where {{ this }}.token_address = source_data.token_address
      and {{ this }}.price_timestamp >= source_data.price_timestamp
)

{% endif %}