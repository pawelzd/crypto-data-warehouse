{{ config(
    materialized='table',
    partition_by={
      "field": "entry_ts",
      "data_type": "timestamp",
      "granularity": "day"
    },
    cluster_by = ["token_address", "chain"]
) }}

WITH strategy AS (
    SELECT * FROM {{ ref('bollinger_indicators_uptrend') }}
),

entries AS (
    SELECT
        token_address,
        chain,
        price_timestamp as entry_ts,
        close as entry_price,
        FARM_FINGERPRINT(CONCAT(token_address, chain, CAST(price_timestamp AS STRING))) as trade_id
    FROM
        strategy
    WHERE
        signal_entry_trend = TRUE
        AND close > 0
),

future_price_action AS (
    SELECT
        e.trade_id,
        e.token_address,
        e.chain,
        e.entry_ts,
        e.entry_price,
        s.price_timestamp,
        s.close,
        s.high,
        s.low,
        s.sma_20,
        
        -- EXIT 1: Profit Target (SMA 20)
        s.signal_target as hit_target,

        -- EXIT 2: Hard Stop (-4% Tightened)
        SAFE_DIVIDE((s.low - e.entry_price), e.entry_price) as low_return_pct

    FROM
        entries e
    JOIN
        strategy s
    ON
        e.token_address = s.token_address 
        AND e.chain = s.chain
        AND s.price_timestamp > e.entry_ts
        -- Max Hold 10 Days
        AND s.price_timestamp < TIMESTAMP_ADD(e.entry_ts, INTERVAL 10 DAY)
),

identified_exits AS (
    SELECT
        trade_id,
        token_address,
        chain,
        entry_ts,
        entry_price,
        
        ARRAY_AGG(
            STRUCT(price_timestamp, close, sma_20, hit_target, low_return_pct) 
            ORDER BY price_timestamp ASC LIMIT 1
        )[OFFSET(0)] as first_event
        
    FROM
        future_price_action
    WHERE
        hit_target = TRUE
        OR
        low_return_pct <= -0.04 -- Tightened Stop Logic
    GROUP BY
        1, 2, 3, 4, 5
),

final_trades AS (
    SELECT
        ex.token_address,
        ex.chain,
        ex.entry_ts,
        ex.entry_price as entry_price_usd,
        ex.first_event.price_timestamp as close_ts,
        
        CASE 
             WHEN ex.first_event.low_return_pct <= -0.04 THEN (ex.entry_price * 0.96)
             WHEN ex.first_event.hit_target THEN ex.first_event.sma_20
             ELSE ex.first_event.close
        END as exit_price_usd,

        CASE 
            WHEN ex.first_event.low_return_pct <= -0.04 THEN 'HARD_STOP'
            WHEN ex.first_event.hit_target THEN 'PROFIT_TARGET_SMA20'
            ELSE 'TIME_EXIT'
        END as exit_reason,

        TIMESTAMP_DIFF(ex.first_event.price_timestamp, ex.entry_ts, HOUR) as bars_to_close
    FROM
        identified_exits ex
)

SELECT
    CONCAT(chain, '-', token_address) as token_chain_id,
    token_address,
    chain,
    CAST(NULL as FLOAT64) as mktcap,
    entry_ts,
    entry_price_usd,
    close_ts,
    exit_price_usd,
    exit_reason,
    SAFE_DIVIDE((exit_price_usd - entry_price_usd), entry_price_usd) as realized_return,
    bars_to_close
FROM
    final_trades f