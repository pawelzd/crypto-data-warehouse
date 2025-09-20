{{ config(
    schema='gold_ml_coins_mon',
    materialized='table'
) }}

SELECT pft.*,
COALESCE(tx_ret_1h, 0) AS tx_ret_1h,
COALESCE(tx_ret_6h, 0) AS tx_ret_6h,
COALESCE(tx_ret_24h, 0) AS tx_ret_24h,
COALESCE(log_tx_ret_1h, 0) AS log_tx_ret_1h,
COALESCE(rel_mean_txs_6h, 0) AS rel_mean_txs_6h,
COALESCE(rel_std_txs_6h, 0) AS rel_std_txs_6h,
COALESCE(rel_mean_txs_24h, 0) AS rel_mean_txs_24h,
COALESCE(rel_std_txs_24h, 0) AS rel_std_txs_24h,
COALESCE(rel_mean_txs_72h, 0) AS rel_mean_txs_72h,
COALESCE(rel_cum_txs_24h, 0) AS rel_cum_txs_24h,
COALESCE(active_ret_1h, 0) AS active_ret_1h,
COALESCE(active_ret_24h, 0) AS active_ret_24h,
COALESCE(log_active_ret_1h, 0) AS log_active_ret_1h,
COALESCE(rel_mean_active_6h, 0) AS rel_mean_active_6h,
COALESCE(rel_std_active_6h, 0) AS rel_std_active_6h,
COALESCE(rel_mean_active_24h, 0) AS rel_mean_active_24h,
COALESCE(rel_std_active_24h, 0) AS rel_std_active_24h,
COALESCE(active_vol_ratio_24_7d, 0) AS active_vol_ratio_24_7d,
COALESCE(active_sharpe_24h, 0) AS active_sharpe_24h,
COALESCE(active_z_24h, 0) AS active_z_24h,
COALESCE(active_drawdown_7d, 0) AS active_drawdown_7d,
COALESCE(new_wallets_ret_1h, 0) AS new_wallets_ret_1h,
COALESCE(log_new_wallets_ret_1h, 0) AS log_new_wallets_ret_1h,
COALESCE(rel_cum_new_wallets_24h, 0) AS rel_cum_new_wallets_24h,
COALESCE(new_wallets_z_12h, 0) AS new_wallets_z_12h,
COALESCE(new_wallets_drawdown_7d, 0) AS new_wallets_drawdown_7d
FROM {{ ref('price_filter_training_dataset_with_market') }} AS pft
LEFT JOIN {{ ref('ml_tokens_trading_activity_features7d') }} AS mta 
ON pft.token_address = mta.token_address
AND pft.decision_ts = mta.ts_hour
