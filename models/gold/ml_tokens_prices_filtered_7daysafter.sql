
WITH prices AS (
    SELECT * FROM {{ ref('ml_tokens_prices_filtered') }}
)

SELECT
    tm.token_address,
    tm.first_acquired_timestamp,
    tm.price_timestamp,
    tm.price_usd,
    tm.price_usd_on_acquisition,
    tm.price_change,
    tm.price_change_pct,
    tm.hours_change_from_acquisition,
    tm.type
FROM
    prices AS tm
WHERE
    tm.price_timestamp BETWEEN tm.first_acquired_timestamp 
                           AND TIMESTAMP_ADD(tm.first_acquired_timestamp, INTERVAL 7 DAY)







