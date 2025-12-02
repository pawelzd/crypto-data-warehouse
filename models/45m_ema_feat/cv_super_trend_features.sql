{{ config(materialized='table') }}

{% set bars_per_day = 32 %}
{% set window_7d = 7 * bars_per_day %}      {# 224 bars ~ 7 days #}
{% set window_30d = 30 * bars_per_day %}    {# 960 bars ~ 30 days #}

WITH ema_bars AS (
    -- This is the output of your Python EMA job
    SELECT
        c.token_chain_id,
        c.token_address,
        c.chain,
        c.price_timestamp,
        o.open,
        o.high,
        o.low,
        o.close       AS price_usd,
        o.volume,
        c.mktcap,
        c.ema_21,
        c.ema_50,
        c.ema_200
    FROM {{ ref('cv_ema21_ema50_calc') }} c
    LEFT JOIN {{ ref('token_ohlcv') }} o
      USING (token_address, chain, price_timestamp)
),

-- 1a) prev_close + true range (no nested analytic)
tr_calc AS (
    SELECT
        e.*,

        LAG(price_usd) OVER (
            PARTITION BY token_chain_id
            ORDER BY price_timestamp
        ) AS prev_close,

        GREATEST(
            high - low,
            ABS(high - LAG(price_usd) OVER (
                PARTITION BY token_chain_id
                ORDER BY price_timestamp
            )),
            ABS(low - LAG(price_usd) OVER (
                PARTITION BY token_chain_id
                ORDER BY price_timestamp
            ))
        ) AS true_range

    FROM ema_bars e
),

-- 1b) ATR(14) as SMA of true_range over 14 bars
atr_calc AS (
    SELECT
        t.*,

        AVG(true_range) OVER (
            PARTITION BY token_chain_id
            ORDER BY price_timestamp
            ROWS BETWEEN 13 PRECEDING AND CURRENT ROW
        ) AS atr_14

    FROM tr_calc t
),

-- 2) Add EMA-derived features (same logic as your Python code, but in SQL)
ema_features AS (
    SELECT
        a.*,

        -- EMA distance
        (ema_21 - ema_50) AS ema_diff_21_50,
        SAFE_DIVIDE(ema_21 - ema_50, price_usd) AS ema_diff_21_50_norm,

        -- EMA 50 slope over last 5 bars
        (ema_50 - LAG(ema_50, 5) OVER (
            PARTITION BY token_chain_id
            ORDER BY price_timestamp
        )) AS ema_50_slope_5,

        -- Local uptrend flag
        CASE
            WHEN ema_21 > ema_50 AND ema_50 > ema_200 THEN TRUE
            ELSE FALSE
        END AS is_local_uptrend,

        -- Normalized ATR
        SAFE_DIVIDE(atr_14, price_usd) AS atr_14_norm

    FROM atr_calc a
),

-- 3) Extra “super trend” helpers (returns, volume, breakouts)
extra_features AS (
    SELECT
        ef.*,

        -- 1-bar return
        SAFE_DIVIDE(
            price_usd,
            LAG(price_usd) OVER (
                PARTITION BY token_chain_id
                ORDER BY price_timestamp
            )
        ) - 1 AS ret_1,

        -- 7d & 30d rolling returns via bar counts (45m bars)
        SAFE_DIVIDE(
            price_usd,
            FIRST_VALUE(price_usd) OVER (
                PARTITION BY token_chain_id
                ORDER BY price_timestamp
                ROWS BETWEEN {{ window_7d - 1 }} PRECEDING AND CURRENT ROW
            )
        ) - 1 AS ret_7d,

        SAFE_DIVIDE(
            price_usd,
            FIRST_VALUE(price_usd) OVER (
                PARTITION BY token_chain_id
                ORDER BY price_timestamp
                ROWS BETWEEN {{ window_30d - 1 }} PRECEDING AND CURRENT ROW
            )
        ) - 1 AS ret_30d,

        -- Volume averages and spikes
        AVG(volume) OVER (
            PARTITION BY token_chain_id
            ORDER BY price_timestamp
            ROWS BETWEEN {{ window_7d - 1 }} PRECEDING AND CURRENT ROW
        ) AS vol_avg_7d,

        AVG(volume) OVER (
            PARTITION BY token_chain_id
            ORDER BY price_timestamp
            ROWS BETWEEN {{ window_30d - 1 }} PRECEDING AND CURRENT ROW
        ) AS vol_avg_30d,

        SAFE_DIVIDE(
            volume,
            AVG(volume) OVER (
                PARTITION BY token_chain_id
                ORDER BY price_timestamp
                ROWS BETWEEN {{ window_7d - 1 }} PRECEDING AND CURRENT ROW
            )
        ) AS vol_spike_7d,

        SAFE_DIVIDE(
            volume,
            AVG(volume) OVER (
                PARTITION BY token_chain_id
                ORDER BY price_timestamp
                ROWS BETWEEN {{ window_30d - 1 }} PRECEDING AND CURRENT ROW
            )
        ) AS vol_spike_30d,

        -- Prior highs for breakout flags (bar-based)
        MAX(high) OVER (
            PARTITION BY token_chain_id
            ORDER BY price_timestamp
            ROWS BETWEEN 20 PRECEDING AND 1 PRECEDING
        ) AS high_20_prev,

        MAX(high) OVER (
            PARTITION BY token_chain_id
            ORDER BY price_timestamp
            ROWS BETWEEN 50 PRECEDING AND 1 PRECEDING
        ) AS high_50_prev

    FROM ema_features ef
),

final AS (
    SELECT
        token_chain_id,
        token_address,
        chain,
        price_timestamp,
        price_usd,
        open,
        high,
        low,
        volume,
        mktcap,

        -- EMAs (from Python)
        ema_21,
        ema_50,
        ema_200,

        -- EMA-derived features
        ema_diff_21_50,
        ema_diff_21_50_norm,
        ema_50_slope_5,
        is_local_uptrend,

        -- Volatility features
        atr_14,
        atr_14_norm,

        -- Extra features (optional, but very useful)
        ret_1,
        ret_7d,
        ret_30d,
        vol_avg_7d,
        vol_avg_30d,
        vol_spike_7d,
        vol_spike_30d,
        high_20_prev,
        high_50_prev,
        CASE WHEN price_usd > high_20_prev THEN TRUE ELSE FALSE END AS is_breakout_20,
        CASE WHEN price_usd > high_50_prev THEN TRUE ELSE FALSE END AS is_breakout_50

    FROM extra_features
)

SELECT * FROM final