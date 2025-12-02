{{ config(
    materialized='table',
    partition_by={
      "field": "price_timestamp",
      "data_type": "timestamp",
      "granularity": "day"
    },
    cluster_by = ["token_address", "chain"]
) }}

WITH base_data AS (
    SELECT
        token_address,
        chain,
        price_timestamp,
        close,
        open,
        high,
        low,
        volume,
        LAG(close) OVER (PARTITION BY token_address, chain ORDER BY price_timestamp) as prev_close
    FROM
        `crypto-trading-474111.core.token_ohlcv`
),

indicators AS (
    SELECT
        *,
        -- SMA 20 (Middle Band)
        AVG(close) OVER (PARTITION BY token_address, chain ORDER BY price_timestamp ROWS BETWEEN 19 PRECEDING AND CURRENT ROW) as sma_20,
        -- STDDEV 20
        STDDEV(close) OVER (PARTITION BY token_address, chain ORDER BY price_timestamp ROWS BETWEEN 19 PRECEDING AND CURRENT ROW) as stddev_20,
        -- SMA 200 (Trend Filter)
        AVG(close) OVER (PARTITION BY token_address, chain ORDER BY price_timestamp ROWS BETWEEN 199 PRECEDING AND CURRENT ROW) as sma_200,
        -- Volume SMA 20 (For panic detection)
        AVG(volume) OVER (PARTITION BY token_address, chain ORDER BY price_timestamp ROWS BETWEEN 19 PRECEDING AND CURRENT ROW) as vol_sma_20,
        -- RSI Gain/Loss
        CASE WHEN close > prev_close THEN close - prev_close ELSE 0 END as gain,
        CASE WHEN close < prev_close THEN prev_close - close ELSE 0 END as loss
    FROM
        base_data
),

rsi_calc AS (
    SELECT
        *,
        AVG(gain) OVER (PARTITION BY token_address, chain ORDER BY price_timestamp ROWS BETWEEN 13 PRECEDING AND CURRENT ROW) as avg_gain,
        AVG(loss) OVER (PARTITION BY token_address, chain ORDER BY price_timestamp ROWS BETWEEN 13 PRECEDING AND CURRENT ROW) as avg_loss,
        -- NEW: Calculate SMA 200 from 24 hours ago to check trend slope
        LAG(sma_200, 24) OVER (PARTITION BY token_address, chain ORDER BY price_timestamp) as sma_200_prev_24h
    FROM
        indicators
),

final_features AS (
    SELECT
        *,
        (sma_20 + (2 * stddev_20)) as bb_upper,
        (sma_20 - (2 * stddev_20)) as bb_lower,
        
        -- Volume Ratio: How much higher is current vol than average?
        CASE WHEN vol_sma_20 > 0 THEN volume / vol_sma_20 ELSE 0 END as vol_ratio,

        -- METRIC: Potential Return to Mean
        CASE WHEN close > 0 THEN (sma_20 - close) / close ELSE 0 END as potential_mean_rev_pct,

        CASE 
            WHEN avg_loss = 0 THEN 100
            ELSE 100 - (100 / (1 + (avg_gain / avg_loss)))
        END as rsi_14
    FROM
        rsi_calc
)

SELECT
    *,
    -- OPTIMIZED ENTRY SIGNAL "STRONG TREND CAPITULATION":
    -- 1. Trend is UP (Price > SMA 200).
    -- 2. NEW: Trend is STRENGTHENING (Slope is Up). This filters out "rollover" tops.
    -- 3. Price is CHEAP (Below Lower Band).
    -- 4. PANIC is DEEP (Volume > 1.5x Average). Increased from 1.25 to ensure true capitulation.
    -- 5. Momentum is OVERSOLD (RSI < 30).
    -- 6. REWARD is HIGH (Potential > 4%). Increased from 3% to ensure we only swing at "fat pitches".
    (
        close > sma_200 AND
        sma_200 > sma_200_prev_24h AND
        close < bb_lower AND 
        vol_ratio > 1.5 AND
        rsi_14 < 30 AND
        potential_mean_rev_pct > 0.04
    ) as signal_entry_optimized,
    
    (close >= sma_20) as signal_exit_target

FROM
    final_features