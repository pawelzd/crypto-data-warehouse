with wallet_history as (
    select * from {{ ref('stg_solana_wallet_history') }}
),

dim_mint_metadata as (
    select * from {{ ref('dim_mint_metadata') }}
),

joined_and_ranked as (
    select
        wh.wallet_address,
        wh.token_address,
        wh.load_id,
        wh.transaction_count,
        wh.asset_amount,
        wh.first_acquired_timestamp,
        wh.load_date,
        dmm.mint_metadata_sk

    from wallet_history as wh

    left join dim_token_metadata as dmm
        on wh.token_address = dmm.token_address
        and dmm.dbt_valid_to is null

)

select * from joined_and_ranked

{% if is_incremental() %}
    where load_date > (select max(load_date) from {{ this }})
{% endif %}