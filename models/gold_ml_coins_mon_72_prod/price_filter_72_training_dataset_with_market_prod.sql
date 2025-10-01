{{ config(
    schema='gold_ml_coins_mon_72_prod',
    materialized='view'
) }}


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
     {{ ref('price_filter_72_training_dataset_prod') }} AS bt
LEFT JOIN
    {{ ref('ml_solana_price_features7d_prod') }} AS mt
    ON  bt.decision_ts = mt.ts_hour

