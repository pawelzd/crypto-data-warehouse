WITH base AS (
    SELECT
        token_address,
        first_acquired_timestamp AS ts_hour,
        buy_txs_1h,
        sell_txs_1h,
        active_wallets_1h,
        new_buyer_wallets_1h,
        (buy_txs_1h + sell_txs_1h) AS total_txs_1h
    FROM {{ ref('fct__token_trading_activity') }}
),
calc AS (
    SELECT
        token_address,
        ts_hour,
        -- TRANSACTION FEATURES

        -- Growth/returns
        SAFE_DIVIDE(total_txs_1h - LAG(total_txs_1h) OVER (PARTITION BY token_address ORDER BY ts_hour),
                    LAG(total_txs_1h) OVER (PARTITION BY token_address ORDER BY ts_hour)) AS tx_ret_1h,
        CASE 
            WHEN total_txs_1h > 0 AND LAG(total_txs_1h) OVER (PARTITION BY token_address ORDER BY ts_hour) > 0
            THEN LN(total_txs_1h / LAG(total_txs_1h) OVER (PARTITION BY token_address ORDER BY ts_hour))
            ELSE NULL
        END AS log_tx_ret_1h,

        -- Multi-horizon returns
        SAFE_DIVIDE(total_txs_1h, NULLIF(LAG(total_txs_1h, 6) OVER (PARTITION BY token_address ORDER BY ts_hour),0)) - 1 AS tx_ret_6h,
        SAFE_DIVIDE(total_txs_1h, NULLIF(LAG(total_txs_1h, 24) OVER (PARTITION BY token_address ORDER BY ts_hour),0)) - 1 AS tx_ret_24h,

        -- Rolling stats (relative to 7d baseline)
        SAFE_DIVIDE(AVG(total_txs_1h) OVER w4,  NULLIF(AVG(total_txs_1h) OVER w168,0)) AS rel_mean_txs_4h,
        SAFE_DIVIDE(STDDEV(total_txs_1h) OVER w4, NULLIF(STDDEV(total_txs_1h) OVER w168,0)) AS rel_std_txs_4h,
        SAFE_DIVIDE(AVG(total_txs_1h) OVER w6,  NULLIF(AVG(total_txs_1h) OVER w168,0)) AS rel_mean_txs_6h,
        SAFE_DIVIDE(STDDEV(total_txs_1h) OVER w6, NULLIF(STDDEV(total_txs_1h) OVER w168,0)) AS rel_std_txs_6h,
        SAFE_DIVIDE(AVG(total_txs_1h) OVER w12, NULLIF(AVG(total_txs_1h) OVER w168,0)) AS rel_mean_txs_12h,
        SAFE_DIVIDE(STDDEV(total_txs_1h) OVER w12, NULLIF(STDDEV(total_txs_1h) OVER w168,0)) AS rel_std_txs_12h,
        SAFE_DIVIDE(AVG(total_txs_1h) OVER w24, NULLIF(AVG(total_txs_1h) OVER w168,0)) AS rel_mean_txs_24h,
        SAFE_DIVIDE(STDDEV(total_txs_1h) OVER w24, NULLIF(STDDEV(total_txs_1h) OVER w168,0)) AS rel_std_txs_24h,
        SAFE_DIVIDE(AVG(total_txs_1h) OVER w72, NULLIF(AVG(total_txs_1h) OVER w168,0)) AS rel_mean_txs_72h,
        SAFE_DIVIDE(STDDEV(total_txs_1h) OVER w72, NULLIF(STDDEV(total_txs_1h) OVER w168,0)) AS rel_std_txs_72h,

        -- Cumulative normalized by 7d
        SAFE_DIVIDE(SUM(total_txs_1h) OVER w4,  NULLIF(SUM(total_txs_1h) OVER w168,0)) AS rel_cum_txs_4h,
        SAFE_DIVIDE(SUM(total_txs_1h) OVER w6,  NULLIF(SUM(total_txs_1h) OVER w168,0)) AS rel_cum_txs_6h,
        SAFE_DIVIDE(SUM(total_txs_1h) OVER w12, NULLIF(SUM(total_txs_1h) OVER w168,0)) AS rel_cum_txs_12h,
        SAFE_DIVIDE(SUM(total_txs_1h) OVER w24, NULLIF(SUM(total_txs_1h) OVER w168,0)) AS rel_cum_txs_24h,
        SAFE_DIVIDE(SUM(total_txs_1h) OVER w72, NULLIF(SUM(total_txs_1h) OVER w168,0)) AS rel_cum_txs_72h,

        -- Volatility ratios
        SAFE_DIVIDE(STDDEV(total_txs_1h) OVER w6,  STDDEV(total_txs_1h) OVER w24) AS tx_vol_ratio_6_24h,
        SAFE_DIVIDE(STDDEV(total_txs_1h) OVER w12, STDDEV(total_txs_1h) OVER w72) AS tx_vol_ratio_12_72h,
        SAFE_DIVIDE(STDDEV(total_txs_1h) OVER w24, STDDEV(total_txs_1h) OVER w168) AS tx_vol_ratio_24_7d,

        -- Sharpe-like ratios
        SAFE_DIVIDE(AVG(total_txs_1h) OVER w4,  STDDEV(total_txs_1h) OVER w4) AS tx_sharpe_4h,
        SAFE_DIVIDE(AVG(total_txs_1h) OVER w6,  STDDEV(total_txs_1h) OVER w6) AS tx_sharpe_6h,
        SAFE_DIVIDE(AVG(total_txs_1h) OVER w12, STDDEV(total_txs_1h) OVER w12) AS tx_sharpe_12h,
        SAFE_DIVIDE(AVG(total_txs_1h) OVER w24, STDDEV(total_txs_1h) OVER w24) AS tx_sharpe_24h,

        -- Z-scores
        SAFE_DIVIDE(total_txs_1h - AVG(total_txs_1h) OVER w6,  STDDEV(total_txs_1h) OVER w6) AS tx_z_6h,
        SAFE_DIVIDE(total_txs_1h - AVG(total_txs_1h) OVER w12, STDDEV(total_txs_1h) OVER w12) AS tx_z_12h,
        SAFE_DIVIDE(total_txs_1h - AVG(total_txs_1h) OVER w24, STDDEV(total_txs_1h) OVER w24) AS tx_z_24h,

        -- Drawdown
        SAFE_DIVIDE(total_txs_1h - MAX(total_txs_1h) OVER w168, MAX(total_txs_1h) OVER w168) AS tx_drawdown_7d,

        -- ACTIVE WALLETS FEATURES

        -- Growth/returns
        SAFE_DIVIDE(active_wallets_1h - LAG(active_wallets_1h) OVER (PARTITION BY token_address ORDER BY ts_hour),
                    LAG(active_wallets_1h) OVER (PARTITION BY token_address ORDER BY ts_hour)) AS active_ret_1h,
        CASE 
            WHEN active_wallets_1h > 0 AND LAG(active_wallets_1h) OVER (PARTITION BY token_address ORDER BY ts_hour) > 0
            THEN LN(active_wallets_1h / LAG(active_wallets_1h) OVER (PARTITION BY token_address ORDER BY ts_hour))
            ELSE NULL
        END AS log_active_ret_1h,

        -- Multi-horizon returns
        SAFE_DIVIDE(active_wallets_1h, NULLIF(LAG(active_wallets_1h, 6) OVER (PARTITION BY token_address ORDER BY ts_hour),0)) - 1 AS active_ret_6h,
        SAFE_DIVIDE(active_wallets_1h, NULLIF(LAG(active_wallets_1h, 24) OVER (PARTITION BY token_address ORDER BY ts_hour),0)) - 1 AS active_ret_24h,

        -- Rolling stats (relative to 7d)
        SAFE_DIVIDE(AVG(active_wallets_1h) OVER w4,  NULLIF(AVG(active_wallets_1h) OVER w168,0)) AS rel_mean_active_4h,
        SAFE_DIVIDE(STDDEV(active_wallets_1h) OVER w4, NULLIF(STDDEV(active_wallets_1h) OVER w168,0)) AS rel_std_active_4h,
        SAFE_DIVIDE(AVG(active_wallets_1h) OVER w6,  NULLIF(AVG(active_wallets_1h) OVER w168,0)) AS rel_mean_active_6h,
        SAFE_DIVIDE(STDDEV(active_wallets_1h) OVER w6, NULLIF(STDDEV(active_wallets_1h) OVER w168,0)) AS rel_std_active_6h,
        SAFE_DIVIDE(AVG(active_wallets_1h) OVER w12, NULLIF(AVG(active_wallets_1h) OVER w168,0)) AS rel_mean_active_12h,
        SAFE_DIVIDE(STDDEV(active_wallets_1h) OVER w12, NULLIF(STDDEV(active_wallets_1h) OVER w168,0)) AS rel_std_active_12h,
        SAFE_DIVIDE(AVG(active_wallets_1h) OVER w24, NULLIF(AVG(active_wallets_1h) OVER w168,0)) AS rel_mean_active_24h,
        SAFE_DIVIDE(STDDEV(active_wallets_1h) OVER w24, NULLIF(STDDEV(active_wallets_1h) OVER w168,0)) AS rel_std_active_24h,
        SAFE_DIVIDE(AVG(active_wallets_1h) OVER w72, NULLIF(AVG(active_wallets_1h) OVER w168,0)) AS rel_mean_active_72h,
        SAFE_DIVIDE(STDDEV(active_wallets_1h) OVER w72, NULLIF(STDDEV(active_wallets_1h) OVER w168,0)) AS rel_std_active_72h,

        -- Cumulative normalized by 7d
        SAFE_DIVIDE(SUM(active_wallets_1h) OVER w4,  NULLIF(SUM(active_wallets_1h) OVER w168,0)) AS rel_cum_active_4h,
        SAFE_DIVIDE(SUM(active_wallets_1h) OVER w6,  NULLIF(SUM(active_wallets_1h) OVER w168,0)) AS rel_cum_active_6h,
        SAFE_DIVIDE(SUM(active_wallets_1h) OVER w12, NULLIF(SUM(active_wallets_1h) OVER w168,0)) AS rel_cum_active_12h,
        SAFE_DIVIDE(SUM(active_wallets_1h) OVER w24, NULLIF(SUM(active_wallets_1h) OVER w168,0)) AS rel_cum_active_24h,

        -- Volatility ratios
        SAFE_DIVIDE(STDDEV(active_wallets_1h) OVER w6,  STDDEV(active_wallets_1h) OVER w24) AS active_vol_ratio_6_24h,
        SAFE_DIVIDE(STDDEV(active_wallets_1h) OVER w12, STDDEV(active_wallets_1h) OVER w72) AS active_vol_ratio_12_72h,
        SAFE_DIVIDE(STDDEV(active_wallets_1h) OVER w24, STDDEV(active_wallets_1h) OVER w168) AS active_vol_ratio_24_7d,

        -- Sharpe-like ratios
        SAFE_DIVIDE(AVG(active_wallets_1h) OVER w4,  STDDEV(active_wallets_1h) OVER w4) AS active_sharpe_4h,
        SAFE_DIVIDE(AVG(active_wallets_1h) OVER w6,  STDDEV(active_wallets_1h) OVER w6) AS active_sharpe_6h,
        SAFE_DIVIDE(AVG(active_wallets_1h) OVER w12, STDDEV(active_wallets_1h) OVER w12) AS active_sharpe_12h,
        SAFE_DIVIDE(AVG(active_wallets_1h) OVER w24, STDDEV(active_wallets_1h) OVER w24) AS active_sharpe_24h,

        -- Z-scores
        SAFE_DIVIDE(active_wallets_1h - AVG(active_wallets_1h) OVER w6,  STDDEV(active_wallets_1h) OVER w6) AS active_z_6h,
        SAFE_DIVIDE(active_wallets_1h - AVG(active_wallets_1h) OVER w12, STDDEV(active_wallets_1h) OVER w12) AS active_z_12h,
        SAFE_DIVIDE(active_wallets_1h - AVG(active_wallets_1h) OVER w24, STDDEV(active_wallets_1h) OVER w24) AS active_z_24h,

        -- Drawdown
        SAFE_DIVIDE(active_wallets_1h - MAX(active_wallets_1h) OVER w168, MAX(active_wallets_1h) OVER w168) AS active_drawdown_7d,

        -- NEW BUYER WALLETS FEATURES

        -- Growth/returns
        SAFE_DIVIDE(new_buyer_wallets_1h - LAG(new_buyer_wallets_1h) OVER (PARTITION BY token_address ORDER BY ts_hour),
                    LAG(new_buyer_wallets_1h) OVER (PARTITION BY token_address ORDER BY ts_hour)) AS new_wallets_ret_1h,
        CASE 
            WHEN new_buyer_wallets_1h > 0 AND LAG(new_buyer_wallets_1h) OVER (PARTITION BY token_address ORDER BY ts_hour) > 0
            THEN LN(new_buyer_wallets_1h / LAG(new_buyer_wallets_1h) OVER (PARTITION BY token_address ORDER BY ts_hour))
            ELSE NULL
        END AS log_new_wallets_ret_1h,  

        -- Rolling stats (relative)
        SAFE_DIVIDE(AVG(new_buyer_wallets_1h) OVER w4,  NULLIF(AVG(new_buyer_wallets_1h) OVER w168,0)) AS rel_mean_new_wallets_4h,
        SAFE_DIVIDE(STDDEV(new_buyer_wallets_1h) OVER w4, NULLIF(STDDEV(new_buyer_wallets_1h) OVER w168,0)) AS rel_std_new_wallets_4h,

        -- Cumulative normalized
        SAFE_DIVIDE(SUM(new_buyer_wallets_1h) OVER w24, NULLIF(SUM(new_buyer_wallets_1h) OVER w168,0)) AS rel_cum_new_wallets_24h,

        -- Z-score
        SAFE_DIVIDE(new_buyer_wallets_1h - AVG(new_buyer_wallets_1h) OVER w12,
                    STDDEV(new_buyer_wallets_1h) OVER w12) AS new_wallets_z_12h,

        -- Drawdown
        SAFE_DIVIDE(new_buyer_wallets_1h - MAX(new_buyer_wallets_1h) OVER w168, MAX(new_buyer_wallets_1h) OVER w168) AS new_wallets_drawdown_7d

    FROM base
    WINDOW
        w4   AS (PARTITION BY token_address ORDER BY ts_hour ROWS BETWEEN 3  PRECEDING AND CURRENT ROW),
        w6   AS (PARTITION BY token_address ORDER BY ts_hour ROWS BETWEEN 5  PRECEDING AND CURRENT ROW),
        w12  AS (PARTITION BY token_address ORDER BY ts_hour ROWS BETWEEN 11 PRECEDING AND CURRENT ROW),
        w24  AS (PARTITION BY token_address ORDER BY ts_hour ROWS BETWEEN 23 PRECEDING AND CURRENT ROW),
        w72  AS (PARTITION BY token_address ORDER BY ts_hour ROWS BETWEEN 71 PRECEDING AND CURRENT ROW),
        w168 AS (PARTITION BY token_address ORDER BY ts_hour ROWS BETWEEN 167 PRECEDING AND CURRENT ROW)
)
SELECT * FROM calc
