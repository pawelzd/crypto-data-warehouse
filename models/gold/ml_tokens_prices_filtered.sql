{{ config(
    materialized='table'
) }}

with tokens as (
    select * from {{ ref('ml_highly_held_tokens_mv') }}
),

prices AS (
    SELECT * FROM {{ ref('ml_token_prices_mv') }}
),
prices_joined AS (
SELECT
    wh.token_address,
    wh.first_acquired_timestamp,
    tm.price_timestamp,
    tm.price_usd,
    wh.type
FROM
    tokens AS wh
INNER JOIN
    prices AS tm
    ON wh.token_address = tm.token_address
WHERE
    (tm.price_timestamp BETWEEN TIMESTAMP_SUB(wh.first_acquired_timestamp, INTERVAL 7 DAY) 
                           AND wh.first_acquired_timestamp) 
    OR (tm.price_timestamp BETWEEN wh.first_acquired_timestamp 
                           AND TIMESTAMP_ADD(wh.first_acquired_timestamp, INTERVAL 7 DAY))
),
prices_joined_ondate AS (
SELECT
    wh.token_address,
    wh.first_acquired_timestamp,
    wh.price_timestamp,
    wh.price_usd
FROM
    prices_joined AS wh
WHERE wh.first_acquired_timestamp = wh.price_timestamp
)

SELECT pj.*, 
        pjd.price_usd as price_usd_on_acquisition, 
        (pj.price_usd - pjd.price_usd) as price_change,
        SAFE_DIVIDE(pj.price_usd - pjd.price_usd, pj.price_usd) as price_change_pct,
        TIMESTAMP_DIFF(pj.price_timestamp, pj.first_acquired_timestamp, HOUR) as hours_change_from_acquisition
FROM prices_joined AS pj
LEFT JOIN prices_joined_ondate AS pjd
ON pj.token_address = pjd.token_address and pj.first_acquired_timestamp = pjd.first_acquired_timestamp

