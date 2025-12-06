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
        AVG(close) OVER (PARTITION BY token_address, chain ORDER BY price_timestamp ROWS BETWEEN 19 PRECEDING AND CURRENT ROW) as sma_20,
        STDDEV(close) OVER (PARTITION BY token_address, chain ORDER BY price_timestamp ROWS BETWEEN 19 PRECEDING AND CURRENT ROW) as stddev_20,
        AVG(close) OVER (PARTITION BY token_address, chain ORDER BY price_timestamp ROWS BETWEEN 1199 PRECEDING AND CURRENT ROW) as sma_1200,
        AVG(volume) OVER (PARTITION BY token_address, chain ORDER BY price_timestamp ROWS BETWEEN 19 PRECEDING AND CURRENT ROW) as vol_sma_20,
        CASE WHEN close > prev_close THEN close - prev_close ELSE 0 END as gain,
        CASE WHEN close < prev_close THEN prev_close - close ELSE 0 END as loss
    FROM
        base_data
),

rsi_calc AS (
    SELECT
        *,
        AVG(gain) OVER (PARTITION BY token_address, chain ORDER BY price_timestamp ROWS BETWEEN 13 PRECEDING AND CURRENT ROW) as avg_gain,
        AVG(loss) OVER (PARTITION BY token_address, chain ORDER BY price_timestamp ROWS BETWEEN 13 PRECEDING AND CURRENT ROW) as avg_loss
    FROM
        indicators
),

features AS (
    SELECT
        *,
        CASE 
            WHEN avg_loss = 0 THEN 100
            ELSE 100 - (100 / (1 + (avg_gain / avg_loss)))
        END as rsi_14,
        (sma_20 + (2 * stddev_20)) as bb_upper,
        (sma_20 - (2 * stddev_20)) as bb_lower,
        LAG(sma_1200, 24) OVER (PARTITION BY token_address, chain ORDER BY price_timestamp) as sma_1200_prev_24h,
        CASE WHEN vol_sma_20 > 0 THEN volume / vol_sma_20 ELSE 0 END as vol_ratio
    FROM
        rsi_calc
)

SELECT
    *,
    -- SIGNAL: THE RISK-ADJUSTED SNIPER
    (
        -- 1. MACRO TREND UP
        sma_1200 > sma_1200_prev_24h
        AND
        -- 2. DEEP VALUE
        close < bb_lower
        AND
        -- 3. OVERSOLD
        rsi_14 < 30
        AND
        -- 4. FAT PITCH > 4% (Quality Filter)
        -- We demand a minimum 4% distance to the mean.
        ((sma_20 - close) / close) > 0.04
        AND
        -- 5. VOLUME
        vol_ratio > 1.2
    ) as signal_entry_trend,

    -- EXIT TARGET: SMA 20
    (high >= sma_20) as signal_target

FROM
    features