{{ config(materialized='table') }}

WITH candles AS (
    -- Bar-level data with EMAs & features
    SELECT
        token_chain_id,
        token_address,
        chain,
        price_timestamp,
        price_usd,
        mktcap,

        -- EMAs
        ema_21,
        ema_50,
        ema_200,

        -- Volatility
        atr_14,
        atr_14_norm,

        -- EMA-derived features
        ema_diff_21_50,
        ema_diff_21_50_norm,
        ema_50_slope_5,
        is_local_uptrend,

        -- Returns
        ret_1,
        ret_7d,
        ret_30d,

        -- Volume features
        vol_spike_7d,
        vol_spike_30d,

        -- Breakout flags
        is_breakout_20,
        is_breakout_50
    FROM {{ ref('cv_super_trend_features') }}

    -- 1) Ensure enough history for EMAs / ATR / returns
    WHERE ema_200 IS NOT NULL      -- only use rows where long EMA exists
      -- optionally also:
      -- AND hours_since_first >= 150  -- ~200 x 45m bars, if you want a hard age floor

      -- 2) Market cap filter by chain
      AND (
        (chain = 'sol'  AND mktcap >= 100000000) OR
        (chain <> 'sol' AND mktcap >=  45000000)
      )
),

-- 0) Define "super-trend regime" on each bar
regime AS (
    SELECT
        c.*,
        CASE
            WHEN is_local_uptrend = TRUE
             AND atr_14_norm BETWEEN 0.01 AND 0.15
             AND ret_30d > 0.10
             AND vol_spike_7d BETWEEN 0.5 AND 5
            THEN TRUE
            ELSE FALSE
        END AS in_super_trend_regime
    FROM candles c
),

-- 1) Mark entry / exit signals on each bar
signals AS (
    SELECT
        r.*,

        -- ENTRY: EMA 21 crosses above EMA 50 AND we're in a good regime
        CASE
            WHEN ema_21 > ema_50
             AND LAG(ema_21) OVER (PARTITION BY token_chain_id ORDER BY price_timestamp)
                 <= LAG(ema_50) OVER (PARTITION BY token_chain_id ORDER BY price_timestamp)
             AND in_super_trend_regime = TRUE
            THEN 1 ELSE 0
        END AS is_entry,

        -- EXIT: first sign the trend is no longer healthy
        CASE
            WHEN
                -- EMA 21 crosses back below EMA 50
                (
                    ema_21 < ema_50
                    AND LAG(ema_21) OVER (PARTITION BY token_chain_id ORDER BY price_timestamp)
                        >= LAG(ema_50) OVER (PARTITION BY token_chain_id ORDER BY price_timestamp)
                )
                -- OR local uptrend breaks
                OR is_local_uptrend = FALSE
                -- OR volatility regime explodes
                OR atr_14_norm > 0.20
            THEN 1 ELSE 0
        END AS is_exit

    FROM regime r
),

-- 2) Entry rows (one per entry bar)
entries AS (
    SELECT
        token_chain_id,
        token_address,
        chain,
        mktcap,
        price_timestamp AS entry_ts,
        price_usd       AS entry_price_usd,
        ROW_NUMBER() OVER (
            PARTITION BY token_chain_id
            ORDER BY price_timestamp
        ) AS entry_idx
    FROM signals
    WHERE is_entry = 1
),

-- 3) Candidate exits: for each entry, all future bars where exit condition is true
candidate_exits AS (
    SELECT
        e.token_chain_id,
        e.token_address,
        e.chain,
        e.mktcap,
        e.entry_ts,
        e.entry_price_usd,
        e.entry_idx,
        s.price_timestamp AS close_ts,
        s.price_usd       AS exit_price_usd,
        ROW_NUMBER() OVER (
            PARTITION BY e.token_chain_id, e.entry_idx
            ORDER BY s.price_timestamp
        ) AS exit_rank
    FROM entries e
    JOIN signals s
      ON s.token_chain_id = e.token_chain_id
     AND s.price_timestamp > e.entry_ts
     AND s.is_exit = 1
),

-- 4) First exit after each entry
exits AS (
    SELECT
        token_chain_id,
        token_address,
        chain,
        mktcap,
        entry_ts,
        entry_price_usd,
        entry_idx,
        close_ts,
        exit_price_usd
    FROM candidate_exits
    WHERE exit_rank = 1
),

-- 5) For each trade window, gather all candles between entry and exit
trade_windows AS (
    SELECT
        e.token_chain_id,
        e.token_address,
        e.chain,
        e.entry_ts,
        e.entry_price_usd,
        e.mktcap,
        x.close_ts,
        x.exit_price_usd,
        s.price_timestamp,
        s.price_usd,
        s.atr_14_norm,
        s.atr_14,
        s.ema_diff_21_50,
        s.ema_diff_21_50_norm,
        s.ema_50_slope_5,
        s.is_local_uptrend,
        s.in_super_trend_regime,
        s.ret_7d,
        s.ret_30d,
        s.vol_spike_7d,
        s.vol_spike_30d,
        s.is_breakout_20,
        s.is_breakout_50
    FROM exits x
    JOIN entries e
      ON e.token_chain_id = x.token_chain_id
     AND e.entry_idx      = x.entry_idx
    JOIN signals s
      ON s.token_chain_id = e.token_chain_id
     AND s.price_timestamp BETWEEN e.entry_ts AND x.close_ts
),

-- 6) Aggregate per trade
trade_stats AS (
    SELECT
        token_chain_id,
        token_address,
        chain,
        ANY_VALUE(mktcap)           AS mktcap,
        entry_ts,
        ANY_VALUE(entry_price_usd)  AS entry_price_usd,
        ANY_VALUE(close_ts)         AS close_ts,
        ANY_VALUE(exit_price_usd)   AS exit_price_usd,

        MAX(price_usd)              AS max_price_usd_in_window,
        COUNT(*) - 1                AS bars_to_close,

        -- feature values at entry
        MAX(IF(price_timestamp = entry_ts, atr_14_norm, NULL))      AS atr_14_norm_at_entry,
        MAX(IF(price_timestamp = entry_ts, atr_14,      NULL))      AS atr_14_at_entry,
        MAX(IF(price_timestamp = entry_ts, ema_diff_21_50,      NULL)) AS ema_diff_21_50_at_entry,
        MAX(IF(price_timestamp = entry_ts, ema_diff_21_50_norm, NULL)) AS ema_diff_21_50_norm_at_entry,
        MAX(IF(price_timestamp = entry_ts, ema_50_slope_5,      NULL)) AS ema_50_slope_5_at_entry,
        MAX(IF(price_timestamp = entry_ts, ret_7d, NULL))          AS ret_7d_at_entry,
        MAX(IF(price_timestamp = entry_ts, ret_30d, NULL))         AS ret_30d_at_entry,
        MAX(IF(price_timestamp = entry_ts, vol_spike_7d, NULL))    AS vol_spike_7d_at_entry,
        MAX(IF(price_timestamp = entry_ts, vol_spike_30d, NULL))   AS vol_spike_30d_at_entry,
        MAX(IF(price_timestamp = entry_ts,
               CAST(is_local_uptrend AS INT64), 0)) = 1            AS is_local_uptrend_at_entry,
        MAX(IF(price_timestamp = entry_ts,
               CAST(in_super_trend_regime AS INT64), 0)) = 1       AS in_super_trend_regime_at_entry,
        MAX(IF(price_timestamp = entry_ts,
               CAST(is_breakout_20   AS INT64), 0)) = 1            AS is_breakout_20_at_entry,
        MAX(IF(price_timestamp = entry_ts,
               CAST(is_breakout_50   AS INT64), 0)) = 1            AS is_breakout_50_at_entry,

        -- window summaries
        MAX(atr_14_norm)  AS max_atr_14_norm_in_window,
        AVG(atr_14_norm)  AS avg_atr_14_norm_in_window

    FROM trade_windows
    GROUP BY
        token_chain_id,
        token_address,
        chain,
        entry_ts
),

final AS (
    SELECT
        token_chain_id,
        token_address,
        chain,
        mktcap,
        entry_ts,
        entry_price_usd,
        close_ts,
        exit_price_usd,
        max_price_usd_in_window,
        bars_to_close,

        SAFE_DIVIDE(exit_price_usd, entry_price_usd) - 1 AS realized_return,
        SAFE_DIVIDE(max_price_usd_in_window, entry_price_usd) - 1 AS max_runup,
        SAFE_DIVIDE(exit_price_usd, max_price_usd_in_window) - 1   AS pullback_from_peak,

        atr_14_norm_at_entry,
        atr_14_at_entry,
        ema_diff_21_50_at_entry,
        ema_diff_21_50_norm_at_entry,
        ema_50_slope_5_at_entry,
        ret_7d_at_entry,
        ret_30d_at_entry,
        vol_spike_7d_at_entry,
        vol_spike_30d_at_entry,
        is_local_uptrend_at_entry,
        in_super_trend_regime_at_entry,
        is_breakout_20_at_entry,
        is_breakout_50_at_entry,
        max_atr_14_norm_in_window,
        avg_atr_14_norm_in_window
    FROM trade_stats
)

SELECT * FROM final