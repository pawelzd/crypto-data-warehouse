{{ config(
    materialized='materialized_view'
) }}
WITH wallet_history AS (
    SELECT * FROM {{ ref('fct__wallet_holdings') }}
),

unique_tokens AS (
    SELECT * FROM {{ source('gold', 'unique_tokens_base') }}
)

SELECT DISTINCT
    wh.token_address,
    TIMESTAMP_TRUNC(wh.first_acquired_timestamp, HOUR) AS first_acquired_timestamp
FROM
    wallet_history AS wh
INNER JOIN
    unique_tokens AS tm
    ON wh.token_address = tm.token_address
