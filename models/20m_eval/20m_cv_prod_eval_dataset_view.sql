{{ config(
    materialized = 'view'
) }}

WITH token_caps AS (
    SELECT
        tp.token_address,
        MAX(tm.market_cap_usd)
    FROM {{ ref('cv_prod_eval_dataset') }} AS tp
    JOIN {{ ref('tmp_birdeye_static_data') }} AS tm
        ON tm.token_address = tp.token_address
    GROUP BY
        tp.token_address
    HAVING
        MAX(tm.market_cap_usd) >= 20000000
)

SELECT
    tp.*
FROM {{ ref('cv_prod_eval_dataset') }} AS tp
JOIN token_caps tc
    ON tp.token_address = tc.token_address
