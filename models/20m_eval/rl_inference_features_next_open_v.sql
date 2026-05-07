{{ config(
    materialized='view'
) }}

{% set trade_size_usd = var('trade_size_usd', 250.0) %}

WITH token_supply AS (
  SELECT
    address,
    MAX(SAFE_CAST(circulating_supply AS FLOAT64)) AS circulating_supply
  FROM {{ source('streamed_datapublic', 'public_tokens_to_monitor') }}
  GROUP BY address
),

filled_hours AS (
  SELECT
    address,
    datetime,
    AVG(SAFE_CAST(price AS FLOAT64)) AS price,
    MAX(SAFE_CAST(volume AS FLOAT64)) AS volume
  FROM {{ ref('20m_cv_prod_filled_hours') }}
  GROUP BY address, datetime
),

base AS (
SELECT
  ts.token_address,
  ts.decision_ts AS price_timestamp,
  ts.price AS next_open,
  cv.price AS next_next_open,
  ts.ret_1h,
  ts.logret_1h,
  ts.mean_ret_24h,
  ts.std_ret_24h,
  ts.mean_ret_72h,
  ts.std_ret_72h,
  ts.mean_ret_168h,
  ts.std_ret_168h,
  ts.rv_24h,
  ts.rv_7d,
  ts.sharpe_24h,
  ts.sharpe_7d,
  ts.ret_z_24h,
  ts.cumret_24h,
  ts.cumret_7d,
  ts.price_z_24h,
  ts.pct_in_range_24h,
  ts.dist_to_sma_6h,
  ts.dist_to_sma_12h,
  ts.dist_to_sma_24h,
  ts.dist_to_sma_72h,
  ts.dist_to_sma_168h,
  ts.dist_to_high_24h,
  ts.dist_to_low_24h,
  ts.breakout_high_24h,
  ts.breakout_low_24h,
  ts.drawdown_7d,
  ts.rsi_14,
  ts.rsi_vol_interaction,
  ts.dow_1_sun_7_sat,
  ts.hour_of_day,
  ts.sin_hour,
  ts.cos_hour,
  ts.sin_dow,
  ts.cos_dow,
  ts.vol_ratio_24_7d,
  ts.vol_ratio_24_72,
  ts.vol_ratio_72_168,
  ts.ret_over_rv_12h,
  ts.sma_diff_fast_slow,
  ts.volume_ret_1h,
  ts.volume_ret_24h,
  ts.volume_cv_24h,
  ts.volume_cv_168h,
  ts.volume_spike_ratio_24h_excl,
  ts.volume_z_24h,
  ts.volume_accel_6v24,
  ts.volume_accel_24v168,
  ts.sharpe_delta,
  ts.btc_ret_1h,
  ts.btc_logret_1h,
  ts.btc_mean_ret_24h,
  ts.btc_std_ret_24h,
  ts.btc_mean_ret_72h,
  ts.btc_std_ret_72h,
  ts.btc_mean_ret_168h,
  ts.btc_std_ret_168h,
  ts.btc_rv_24h,
  ts.btc_rv_7d,
  ts.btc_sharpe_24h,
  ts.btc_sharpe_7d,
  ts.btc_ret_z_24h,
  ts.btc_macd_sma_12_26h,
  ts.btc_dist_to_sma_6h,
  ts.btc_dist_to_sma_12h,
  ts.btc_dist_to_sma_24h,
  ts.btc_dist_to_sma_168h,
  ts.btc_pct_in_range_24h,
  ts.btc_dist_to_high_24h,
  ts.btc_dist_to_low_24h,
  ts.btc_breakout_high_24h,
  ts.btc_breakout_low_24h,
  ts.btc_drawdown_7d,
  ts.btc_rsi_14,
  ts.btc_sin_hour,
  ts.btc_cos_hour,
  ts.btc_sin_dow,
  ts.btc_cos_dow,
  ts.btc_vol_ratio_24_7d,
  ts.btc_sharpe_delta,
  ts.spread_ret_1h,
  ts.spread_logret_1h,
  ts.sol_ret_1h,
  ts.sol_logret_1h,
  ts.sol_mean_ret_24h,
  ts.sol_std_ret_24h,
  ts.sol_mean_ret_72h,
  ts.sol_std_ret_72h,
  ts.sol_mean_ret_168h,
  ts.sol_std_ret_168h,
  ts.sol_rv_24h,
  ts.sol_rv_7d,
  ts.sol_sharpe_24h,
  ts.sol_sharpe_7d,
  ts.sol_ret_z_24h,
  ts.sol_cumret_24h,
  ts.sol_cumret_7d,
  ts.sol_macd_sma_12_26h,
  ts.sol_dist_to_sma_6h,
  ts.sol_dist_to_sma_12h,
  ts.sol_dist_to_sma_24h,
  ts.sol_dist_to_sma_72h,
  ts.sol_dist_to_sma_168h,
  ts.sol_pct_in_range_24h,
  ts.sol_dist_to_high_24h,
  ts.sol_dist_to_low_24h,
  ts.sol_breakout_high_24h,
  ts.sol_breakout_low_24h,
  ts.sol_drawdown_7d,
  ts.sol_rsi_14,
  ts.sol_sin_hour,
  ts.sol_cos_hour,
  ts.sol_sin_dow,
  ts.sol_cos_dow,
  ts.sol_vol_ratio_24_7d,
  ts.sol_sharpe_delta,
  ts.std_ret_24h AS std_ret_24h_cost,
  SAFE_CAST(cur.volume AS FLOAT64) AS volume_cost,
  SAFE_CAST(ts.price AS FLOAT64) * s.circulating_supply AS mktcap_cost
FROM {{ ref('20m_cv_prod_eval_dataset') }} ts
LEFT JOIN filled_hours cv
  ON ts.token_address = cv.address
 AND TIMESTAMP_ADD(ts.decision_ts, INTERVAL 1 HOUR) = cv.datetime
LEFT JOIN filled_hours cur
  ON ts.token_address = cur.address
 AND ts.decision_ts = cur.datetime
LEFT JOIN token_supply s
  ON ts.token_address = s.address
),

cost_inputs AS (
  SELECT
    b.*,
    COALESCE(SUM(b.volume_cost) OVER w24, 0.0) * COALESCE(SAFE_CAST(b.next_open AS FLOAT64), 0.0) AS volume_usd_cost,
    COALESCE(AVG(SAFE_DIVIDE(ABS(COALESCE(b.ret_1h, 0)), NULLIF(b.volume_cost, 0))) OVER w24, 0.0) AS amihud_cost
  FROM base b
  WINDOW w24 AS (
    PARTITION BY b.token_address
    ORDER BY b.price_timestamp
    ROWS BETWEEN 23 PRECEDING AND CURRENT ROW
  )
),

cost_intermediates AS (
  SELECT
    *,
    LN(GREATEST(COALESCE(volume_usd_cost, 0.0), 1.0)) AS _log_v,
    LN(GREATEST(COALESCE(mktcap_cost, 0.0), 1.0)) AS _log_mc,
    LN(GREATEST({{ trade_size_usd }} / GREATEST(COALESCE(volume_usd_cost, 0.0), 1.0), 1e-10)) AS _log_tv,
    LN(GREATEST(COALESCE(volume_usd_cost, 0.0) / GREATEST(COALESCE(mktcap_cost, 0.0), 1.0), 1e-10)) AS _log_vm,
    CAST({{ trade_size_usd }} AS FLOAT64) / GREATEST(COALESCE(volume_usd_cost, 0.0), 1.0) AS _trade_to_vol
  FROM cost_inputs
),

cost_cols AS (
  SELECT
    * EXCEPT (_log_v, _log_mc, _log_tv, _log_vm, _trade_to_vol),

    LEAST(
      CAST(
        EXP(
            -0.371
          + 0.573 * LN(GREATEST(CAST({{ trade_size_usd }} AS FLOAT64), 1.0))
          + 0.424 * _log_tv
          + 0.218 * _log_vm
          + 0.149 * _log_v
          - 0.069 * _log_mc
        ) / CAST({{ trade_size_usd }} AS FLOAT64) * 10000.0
      AS FLOAT64),
      500.0
    ) AS fee_bps_default,

    CAST(
      CASE
        WHEN _trade_to_vol > 0.005  THEN 300.0
        WHEN _trade_to_vol > 0.0005 THEN  50.0
        ELSE                              10.0
      END
    AS FLOAT64) AS slippage_bps_proxy
  FROM cost_intermediates
)

SELECT *
FROM cost_cols
