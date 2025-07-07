-- This is a simple test model to check if dbt-utils is working.

select
    {{ dbt_utils.generate_surrogate_key(['token_address', 'price_timestamp']) }} as test_key
from {{ ref('stg_solana_price_history') }}
where false