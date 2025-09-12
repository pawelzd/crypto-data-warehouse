with trading_activity as (
    select * from {{ ref('stg_token_trading_activity') }}
)

select
 *
from trading_activity as ta
{% if is_incremental() %}
    where ta.first_acquired_timestamp > (select max(first_acquired_timestamp) from {{ this }})
{% endif %}