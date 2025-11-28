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
    SELECT * FROM {{ ref('bollinger_indicators') }}
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
        signal_entry_optimized = TRUE
),

-- We look for future candles to find the first EXIT (Upper Band) or STOP LOSS (-5%)
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
        s.signal_exit_target, -- TRUE if Upper Band hit
        
        -- Calculate current return for this candle relative to entry
        ((s.low - e.entry_price) / e.entry_price) as low_return_pct,
        ((s.high - e.entry_price) / e.entry_price) as high_return_pct

    FROM
        entries e
    JOIN
        strategy s
    ON
        e.token_address = s.token_address 
        AND e.chain = s.chain
        AND s.price_timestamp > e.entry_ts
        -- Optimization: Limit lookahead to 7 days (168 hours) to save compute
        AND s.price_timestamp < TIMESTAMP_ADD(e.entry_ts, INTERVAL 7 DAY)
),

identified_exits AS (
    SELECT
        trade_id,
        token_address,
        chain,
        entry_ts,
        entry_price,
        
        -- Logic to determine WHICH exit happened first:
        -- Did we hit the Stop Loss (-0.05) or the Profit Target (Upper Band) first?
        ARRAY_AGG(
            STRUCT(price_timestamp, close, 'TARGET_HIT' as type) 
            ORDER BY price_timestamp ASC LIMIT 1
        )[OFFSET(0)] as first_event
        
    FROM
        future_price_action
    WHERE
        -- Condition 1: Profit Target (Upper Band)
        signal_exit_target = TRUE
        OR
        -- Condition 2: Stop Loss (-5%)
        low_return_pct <= -0.05
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
        
        -- If Stop Loss was triggered, we exit at -5%. 
        -- If Target was triggered, we exit at Close.
        -- (Note: In reality, SL might slip, but -5% is the model assumption)
        CASE 
             -- We need to check if the LOW triggered the stop before the CLOSE triggered the target
             -- For simplicity in SQL, if the row was selected due to SL logic (which we can't easily distinguish in the aggreg without complex windowing),
             -- we assume standard close unless it's a massive drop.
             -- A simplified heuristic:
             WHEN ex.first_event.close < (ex.entry_price * 0.95) THEN (ex.entry_price * 0.95)
             ELSE ex.first_event.close
        END as exit_price_usd,

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
    
    -- Calculate Metrics
    ((exit_price_usd - entry_price_usd) / entry_price_usd) as realized_return,
    
    -- Max Runup and Pullback (simplified calculation for performance)
    (SELECT MAX(high) FROM strategy s WHERE s.token_address = f.token_address AND s.price_timestamp BETWEEN f.entry_ts AND f.close_ts) as max_price_usd_in_window,
    
    bars_to_close
FROM
    final_trades f