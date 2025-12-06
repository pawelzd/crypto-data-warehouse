WITH 
-- 1. Engine A: Panic Reversion (Strategy 6)
strategy_panic AS (
    SELECT 
        entry_ts, 
        realized_return, 
        'PANIC_REVERSION' as strategy_type,
        token_chain_id
    FROM {{ ref('bollinger_indicators_trades') }}
    WHERE close_ts IS NOT NULL 
),

-- 2. Engine B: Bull Sniper (Strategy 17)
strategy_sniper AS (
    SELECT 
        entry_ts, 
        realized_return, 
        'BULL_SNIPER' as strategy_type,
        token_chain_id
    FROM {{ ref('bollinger_indicators_uptrend_trades') }}
    WHERE close_ts IS NOT NULL
),

all_trades AS (
    SELECT * FROM strategy_panic
    UNION ALL
    SELECT * FROM strategy_sniper
),

daily_execution AS (
    SELECT
        *,
        -- CAPITAL CONSTRAINT: Rank trades to take max 3 per day
        ROW_NUMBER() OVER (
            PARTITION BY entry_ts 
            ORDER BY CASE WHEN strategy_type = 'PANIC_REVERSION' THEN 1 ELSE 2 END ASC
        ) as daily_rank
    FROM
        all_trades
),

-- STEP 1: Aggregate by Month FIRST (Fixes the Group By error)
monthly_stats AS (
    SELECT
        DATE_TRUNC(entry_ts, MONTH) as month,
        
        -- Strategy Breakdowns
        COUNTIF(strategy_type = 'PANIC_REVERSION') as panic_trades,
        ROUND(AVG(CASE WHEN strategy_type = 'PANIC_REVERSION' THEN realized_return END) * 100, 2) as panic_avg_pct,
        
        COUNTIF(strategy_type = 'BULL_SNIPER') as sniper_trades,
        ROUND(AVG(CASE WHEN strategy_type = 'BULL_SNIPER' THEN realized_return END) * 100, 2) as sniper_avg_pct,
        
        COUNT(*) as total_trades,
        ROUND(SUM(realized_return), 2) as monthly_R_units
    FROM
        daily_execution
    WHERE
        daily_rank <= 3 -- Apply the limit here
    GROUP BY 
        1
)

-- STEP 2: Calculate Cumulative Sums on the aggregated data
SELECT
    *,
    -- The Equity Curve
    SUM(monthly_R_units) OVER (ORDER BY month ASC) as cumulative_R
FROM
    monthly_stats
ORDER BY
    month ASC