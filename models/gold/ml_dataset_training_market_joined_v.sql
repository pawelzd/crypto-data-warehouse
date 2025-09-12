WITH btc_join AS (SELECT 
    dt.*,
    COALESCE(mt.ret_1h, 0) AS btc_ret_1h,
    COALESCE(mt.logret_1h, 0) AS btc_logret_1h,
    COALESCE(mt.mean_ret_24h, 0) AS btc_mean_ret_24h,
    COALESCE(mt.std_ret_24h, 0) AS btc_std_ret_24h,
    COALESCE(mt.mean_ret_72h, 0) AS btc_mean_ret_72h,
    COALESCE(mt.std_ret_72h, 0) AS btc_std_ret_72h,
    COALESCE(mt.mean_ret_168h, 0) AS btc_mean_ret_168h,
    COALESCE(mt.std_ret_168h, 0) AS btc_std_ret_168h,
    COALESCE(mt.rv_24h, 0) AS btc_rv_24h,
    COALESCE(mt.rv_7d, 0) AS btc_rv_7d,
    COALESCE(mt.sharpe_24h, 0) AS btc_sharpe_24h,
    COALESCE(mt.sharpe_7d, 0) AS btc_sharpe_7d,
    COALESCE(mt.ret_z_24h, 0) AS btc_ret_z_24h,
    COALESCE(mt.cumret_24h, 0) AS btc_cumret_24h,
    COALESCE(mt.cumret_7d, 0) AS btc_cumret_7d,
    COALESCE(mt.macd_sma_12_26h, 0) AS btc_macd_sma_12_26h,
    COALESCE(mt.dist_to_sma_6h, 0) AS btc_dist_to_sma_6h,
    COALESCE(mt.dist_to_sma_12h, 0) AS btc_dist_to_sma_12h,
    COALESCE(mt.dist_to_sma_24h, 0) AS btc_dist_to_sma_24h,
    COALESCE(mt.dist_to_sma_72h, 0) AS btc_dist_to_sma_72h,
    COALESCE(mt.dist_to_sma_168h, 0) AS btc_dist_to_sma_168h,
    COALESCE(mt.pct_in_range_24h, 0) AS btc_pct_in_range_24h,
    COALESCE(mt.dist_to_high_24h, 0) AS btc_dist_to_high_24h,
    COALESCE(mt.dist_to_low_24h, 0) AS btc_dist_to_low_24h,
    COALESCE(mt.breakout_high_24h, 0) AS btc_breakout_high_24h,
    COALESCE(mt.breakout_low_24h, 0) AS btc_breakout_low_24h,
    COALESCE(mt.drawdown_7d, 0) AS btc_drawdown_7d,
    COALESCE(mt.rsi_14, 0) AS btc_rsi_14,
    COALESCE(mt.acf1_72h, 0) AS btc_acf1_72h,
    COALESCE(mt.sin_hour, 0) AS btc_sin_hour,
    COALESCE(mt.cos_hour, 0) AS btc_cos_hour,
    COALESCE(mt.sin_dow, 0) AS btc_sin_dow,
    COALESCE(mt.cos_dow, 0) AS btc_cos_dow,
    COALESCE(mt.vol_ratio_24_7d, 0) AS btc_vol_ratio_24_7d,
    COALESCE(mt.sharpe_delta, 0) AS btc_sharpe_delta
FROM
    {{ ref('ml_dataset_training_wo_wallets_v') }} AS dt
INNER JOIN
    {{ ref('ml_market_tokens_features7d_v') }} AS mt
    ON  dt.first_acquired_timestamp = mt.ts_hour
WHERE mt.token_address = "BTCU"),
sol_join AS (
SELECT 
    bt.*,
    COALESCE(mt.ret_1h, 0) AS sol_ret_1h,
    COALESCE(mt.logret_1h, 0) AS sol_logret_1h,
    COALESCE(mt.mean_ret_24h, 0) AS sol_mean_ret_24h,
    COALESCE(mt.std_ret_24h, 0) AS sol_std_ret_24h,
    COALESCE(mt.mean_ret_72h, 0) AS sol_mean_ret_72h,
    COALESCE(mt.std_ret_72h, 0) AS sol_std_ret_72h,
    COALESCE(mt.mean_ret_168h, 0) AS sol_mean_ret_168h,
    COALESCE(mt.std_ret_168h, 0) AS sol_std_ret_168h,
    COALESCE(mt.rv_24h, 0) AS sol_rv_24h,
    COALESCE(mt.rv_7d, 0) AS sol_rv_7d,
    COALESCE(mt.sharpe_24h, 0) AS sol_sharpe_24h,
    COALESCE(mt.sharpe_7d, 0) AS sol_sharpe_7d,
    COALESCE(mt.ret_z_24h, 0) AS sol_ret_z_24h,
    COALESCE(mt.cumret_24h, 0) AS sol_cumret_24h,
    COALESCE(mt.cumret_7d, 0) AS sol_cumret_7d,
    COALESCE(mt.macd_sma_12_26h, 0) AS sol_macd_sma_12_26h,
    COALESCE(mt.dist_to_sma_6h, 0) AS sol_dist_to_sma_6h,
    COALESCE(mt.dist_to_sma_12h, 0) AS sol_dist_to_sma_12h,
    COALESCE(mt.dist_to_sma_24h, 0) AS sol_dist_to_sma_24h,
    COALESCE(mt.dist_to_sma_72h, 0) AS sol_dist_to_sma_72h,
    COALESCE(mt.dist_to_sma_168h, 0) AS sol_dist_to_sma_168h,
    COALESCE(mt.pct_in_range_24h, 0) AS sol_pct_in_range_24h,
    COALESCE(mt.dist_to_high_24h, 0) AS sol_dist_to_high_24h,
    COALESCE(mt.dist_to_low_24h, 0) AS sol_dist_to_low_24h,
    COALESCE(mt.breakout_high_24h, 0) AS sol_breakout_high_24h,
    COALESCE(mt.breakout_low_24h, 0) AS sol_breakout_low_24h,
    COALESCE(mt.drawdown_7d, 0) AS sol_drawdown_7d,
    COALESCE(mt.rsi_14, 0) AS sol_rsi_14,
    COALESCE(mt.acf1_72h, 0) AS sol_acf1_72h,
    COALESCE(mt.sin_hour, 0) AS sol_sin_hour,
    COALESCE(mt.cos_hour, 0) AS sol_cos_hour,
    COALESCE(mt.sin_dow, 0) AS sol_sin_dow,
    COALESCE(mt.cos_dow, 0) AS sol_cos_dow,
    COALESCE(mt.vol_ratio_24_7d, 0) AS sol_vol_ratio_24_7d,
    COALESCE(mt.sharpe_delta, 0) AS sol_sharpe_delta
FROM
    btc_join bt
INNER JOIN
    {{ ref('ml_market_tokens_features7d_v') }} AS mt
    ON  bt.first_acquired_timestamp = mt.ts_hour
WHERE mt.token_address = "SOLU"
)

SELECT 
    sj.*,
    
    -- ==================================================
    -- TRANSACTION FEATURES
    -- ==================================================
    COALESCE(ta.tx_ret_1h, 0) AS tx_ret_1h,
    COALESCE(ta.log_tx_ret_1h, 0) AS log_tx_ret_1h,
    COALESCE(ta.tx_ret_6h, 0) AS tx_ret_6h,
    COALESCE(ta.tx_ret_24h, 0) AS tx_ret_24h,

    COALESCE(ta.rel_mean_txs_4h, 0) AS rel_mean_txs_4h,
    COALESCE(ta.rel_std_txs_4h, 0) AS rel_std_txs_4h,
    COALESCE(ta.rel_mean_txs_6h, 0) AS rel_mean_txs_6h,
    COALESCE(ta.rel_std_txs_6h, 0) AS rel_std_txs_6h,
    COALESCE(ta.rel_mean_txs_12h, 0) AS rel_mean_txs_12h,
    COALESCE(ta.rel_std_txs_12h, 0) AS rel_std_txs_12h,
    COALESCE(ta.rel_mean_txs_24h, 0) AS rel_mean_txs_24h,
    COALESCE(ta.rel_std_txs_24h, 0) AS rel_std_txs_24h,
    COALESCE(ta.rel_mean_txs_72h, 0) AS rel_mean_txs_72h,
    COALESCE(ta.rel_std_txs_72h, 0) AS rel_std_txs_72h,

    COALESCE(ta.rel_cum_txs_4h, 0) AS rel_cum_txs_4h,
    COALESCE(ta.rel_cum_txs_6h, 0) AS rel_cum_txs_6h,
    COALESCE(ta.rel_cum_txs_12h, 0) AS rel_cum_txs_12h,
    COALESCE(ta.rel_cum_txs_24h, 0) AS rel_cum_txs_24h,
    COALESCE(ta.rel_cum_txs_72h, 0) AS rel_cum_txs_72h,

    COALESCE(ta.tx_vol_ratio_6_24h, 0) AS tx_vol_ratio_6_24h,
    COALESCE(ta.tx_vol_ratio_12_72h, 0) AS tx_vol_ratio_12_72h,
    COALESCE(ta.tx_vol_ratio_24_7d, 0) AS tx_vol_ratio_24_7d,

    COALESCE(ta.tx_sharpe_4h, 0) AS tx_sharpe_4h,
    COALESCE(ta.tx_sharpe_6h, 0) AS tx_sharpe_6h,
    COALESCE(ta.tx_sharpe_12h, 0) AS tx_sharpe_12h,
    COALESCE(ta.tx_sharpe_24h, 0) AS tx_sharpe_24h,

    COALESCE(ta.tx_z_6h, 0) AS tx_z_6h,
    COALESCE(ta.tx_z_12h, 0) AS tx_z_12h,
    COALESCE(ta.tx_z_24h, 0) AS tx_z_24h,

    COALESCE(ta.tx_drawdown_7d, 0) AS tx_drawdown_7d,

    -- ==================================================
    -- ACTIVE WALLETS FEATURES
    -- ==================================================
    COALESCE(ta.active_ret_1h, 0) AS active_ret_1h,
    COALESCE(ta.log_active_ret_1h, 0) AS log_active_ret_1h,
    COALESCE(ta.active_ret_6h, 0) AS active_ret_6h,
    COALESCE(ta.active_ret_24h, 0) AS active_ret_24h,

    COALESCE(ta.rel_mean_active_4h, 0) AS rel_mean_active_4h,
    COALESCE(ta.rel_std_active_4h, 0) AS rel_std_active_4h,
    COALESCE(ta.rel_mean_active_6h, 0) AS rel_mean_active_6h,
    COALESCE(ta.rel_std_active_6h, 0) AS rel_std_active_6h,
    COALESCE(ta.rel_mean_active_12h, 0) AS rel_mean_active_12h,
    COALESCE(ta.rel_std_active_12h, 0) AS rel_std_active_12h,
    COALESCE(ta.rel_mean_active_24h, 0) AS rel_mean_active_24h,
    COALESCE(ta.rel_std_active_24h, 0) AS rel_std_active_24h,
    COALESCE(ta.rel_mean_active_72h, 0) AS rel_mean_active_72h,
    COALESCE(ta.rel_std_active_72h, 0) AS rel_std_active_72h,

    COALESCE(ta.rel_cum_active_4h, 0) AS rel_cum_active_4h,
    COALESCE(ta.rel_cum_active_6h, 0) AS rel_cum_active_6h,
    COALESCE(ta.rel_cum_active_12h, 0) AS rel_cum_active_12h,
    COALESCE(ta.rel_cum_active_24h, 0) AS rel_cum_active_24h,

    COALESCE(ta.active_vol_ratio_6_24h, 0) AS active_vol_ratio_6_24h,
    COALESCE(ta.active_vol_ratio_12_72h, 0) AS active_vol_ratio_12_72h,
    COALESCE(ta.active_vol_ratio_24_7d, 0) AS active_vol_ratio_24_7d,

    COALESCE(ta.active_sharpe_4h, 0) AS active_sharpe_4h,
    COALESCE(ta.active_sharpe_6h, 0) AS active_sharpe_6h,
    COALESCE(ta.active_sharpe_12h, 0) AS active_sharpe_12h,
    COALESCE(ta.active_sharpe_24h, 0) AS active_sharpe_24h,

    COALESCE(ta.active_z_6h, 0) AS active_z_6h,
    COALESCE(ta.active_z_12h, 0) AS active_z_12h,
    COALESCE(ta.active_z_24h, 0) AS active_z_24h,

    COALESCE(ta.active_drawdown_7d, 0) AS active_drawdown_7d,

    -- ==================================================
    -- NEW BUYER WALLETS FEATURES
    -- ==================================================
    COALESCE(ta.new_wallets_ret_1h, 0) AS new_wallets_ret_1h,
    COALESCE(ta.log_new_wallets_ret_1h, 0) AS log_new_wallets_ret_1h,

    COALESCE(ta.rel_mean_new_wallets_4h, 0) AS rel_mean_new_wallets_4h,
    COALESCE(ta.rel_std_new_wallets_4h, 0) AS rel_std_new_wallets_4h,

    COALESCE(ta.rel_cum_new_wallets_24h, 0) AS rel_cum_new_wallets_24h,

    COALESCE(ta.new_wallets_z_12h, 0) AS new_wallets_z_12h,
    COALESCE(ta.new_wallets_drawdown_7d, 0) AS new_wallets_drawdown_7d

FROM sol_join sj
LEFT JOIN {{ ref('ml_tokens_trading_activity_features7d') }} AS ta
    ON sj.token_address = ta.token_address
   AND sj.first_acquired_timestamp = ta.ts_hour


