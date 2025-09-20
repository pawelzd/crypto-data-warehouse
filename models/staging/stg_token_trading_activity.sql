with source as (
    select * from {{ source('solana_raw_prices', 'raw_wallet_history_features') }}
)

SELECT 
    _col_0 AS token_address,
    _col_1 AS first_acquired_timestamp,
    _col_2 AS delta_buy_bal_1h,
    _col_3 AS delta_sell_bal_1h,
    _col_4 AS delta_0_bal_1h,
    _col_5 AS buy_txs_1h,
    _col_6 AS sell_txs_1h,
    _col_7 AS active_wallets_1h,
    _col_8 AS new_buyer_wallets_1h,
    _col_9 AS wallet_delta_buy_bal_stddev_1h,
    _col_10 AS wallet_delta_sell_bal_stddev_1h,
    _col_11 AS wallet_buy_txs_stddev_1h,
    _col_12 AS wallet_sell_txs_stddev_1h
FROM source
WHERE _col_1 >= '2022-01-01 00:00:00+00'
  AND _col_0 IS NOT NULL
  AND _col_1 IS NOT NULL