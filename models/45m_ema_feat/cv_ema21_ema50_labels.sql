WITH base AS (
    SELECT
        chain,
        token_chain_id,
        token_address,
        price_timestamp,
        price_usd,
        volume,
        mktcap,
        ema_21,
        ema_50,

        -- previous hour EMA (shift(1) equivalent)
        LAG(ema_21) OVER (
            PARTITION BY token_chain_id
            ORDER BY price_timestamp
        ) AS ema_21_prev_1h,

        LAG(ema_50) OVER (
            PARTITION BY token_chain_id
            ORDER BY price_timestamp
        ) AS ema_50_prev_1h,

        -- hours since first price_timestamp
        TIMESTAMP_DIFF(
            price_timestamp,
            FIRST_VALUE(price_timestamp) OVER (
                PARTITION BY token_chain_id
                ORDER BY price_timestamp
            ),
            HOUR
        ) AS hours_since_first
    FROM {{ source('45m_ema_feat', 'cv_ema21_ema50_calc') }}
),

labels AS (
    SELECT
        *,
        CASE 
            WHEN hours_since_first >= 50
             AND ema_21_prev_1h >= ema_50_prev_1h
             AND ema_21 < ema_50
            THEN 1
            ELSE 0
        END AS label_close,

        CASE 
            WHEN hours_since_first >= 50
             AND ema_21_prev_1h <= ema_50_prev_1h
             AND ema_21 > ema_50
            THEN 1
            ELSE 0
        END AS label_entry
    FROM base
)

SELECT * FROM labels
WHERE hours_since_first >= 50
AND (mktcap >= 100000000 AND chain = 'sol') OR (chain <> 'sol' AND mktcap >= 45000000)
