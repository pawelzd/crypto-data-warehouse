{{ config(
    schema='gold_ml_coins_mon_72',
    materialized='table'
) }}

{{ config(
    schema='gold_ml_coins_mon_72',
    materialized='table'
) }}

SELECT
  pft.*,


  -- === Key volume features (lean set) with COALESCE defaults ===
  COALESCE(mta.log_volume, 0) AS log_volume,
  COALESCE(mta.log_volume_per_supply, 0) AS log_volume_per_supply,

  COALESCE(mta.vol_ret_1h,  0) AS vol_ret_1h,
  COALESCE(mta.vol_ret_24h, 0) AS vol_ret_24h,

  COALESCE(mta.log_vol_mean_24h,  0) AS log_vol_mean_24h,
  COALESCE(mta.log_vol_mean_168h, 0) AS log_vol_mean_168h,
  COALESCE(mta.log_vol_mean_24h_per_supply,  0) AS log_vol_mean_24h_per_supply,
  COALESCE(mta.log_vol_mean_168h_per_supply, 0) AS log_vol_mean_168h_per_supply,

  COALESCE(mta.vol_cv_24h,  0) AS vol_cv_24h,
  COALESCE(mta.vol_cv_168h, 0) AS vol_cv_168h,

  COALESCE(mta.vol_spike_ratio_24h_excl, 0) AS vol_spike_ratio_24h_excl,
  COALESCE(mta.vol_z_24h,               0) AS vol_z_24h,

  COALESCE(mta.vol_accel_6v24,   0) AS vol_accel_6v24,
  COALESCE(mta.vol_accel_24v168, 0) AS vol_accel_24v168,

  COALESCE(mta.vol_autocorr1_24h, 0) AS vol_autocorr1_24h,

  COALESCE(mta.vol_ema_fast, 0) AS vol_ema_fast,
  COALESCE(mta.vol_ema_slow, 0) AS vol_ema_slow,

  CASE
    WHEN COALESCE(mta.vol_ret_24h, 0) >0 THEN label_profit20_before_loss25
    ELSE 0
  END AS label_profit20_before_loss25,
  


FROM {{ ref('price_filter_72_training_dataset_with_market') }} AS pft
LEFT JOIN {{ ref('ml_tokens_72_trading_activity_features7d') }} AS mta
  ON pft.token_address = mta.token_address
 AND pft.decision_ts  = mta.ts_hour
