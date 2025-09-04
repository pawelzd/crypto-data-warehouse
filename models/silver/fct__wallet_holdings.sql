-- with wallet_history as (
--     select * from {{ ref('stg_solana_wallets_holdings') }}
-- ),

-- dim_token_metadata as (
--     select * from {{ ref('stg_tokens_metadata') }}
-- ),

-- joined_and_ranked as (
--     select
--         wh.wallet_address,
--         wh.token_address,
--         wh.load_id,
--         wh.transaction_count,
--         wh.asset_amount,
--         wh.first_acquired_timestamp,
--         wh.load_date,
--         dmm.token_metadata_sk

--     from wallet_history as wh

--     left join dim_token_metadata as dmm
--         on wh.token_address = dmm.token_address
--         and dmm.dbt_valid_to is null

-- )

-- select * from joined_and_ranked

-- {% if is_incremental() %}
--     where load_date > (select max(load_date) from {{ this }})
-- {% endif %}

with wallet_history as (
    select * from {{ ref('stg_solana_wallets_holdings') }}
),

token_metadata as (
    select * from {{ ref('stg_tokens_metadata') }}
)

select
    wh.wallet_address,
    wh.token_address,
    wh.transaction_count,
    wh.asset_amount,
    wh.first_acquired_timestamp,
    wh.last_acquired_timestamp,
    wh.first_transaction_id,
    wh.last_transaction_id,
    tm.token_metadata_sk
from wallet_history as wh

left join token_metadata as tm
on wh.token_address = tm.token_address

{% if is_incremental() %}
    where last_acquired_timestamp > (select max(last_acquired_timestamp) from {{ this }})
{% endif %}