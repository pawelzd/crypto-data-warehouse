{{ config(
    materialized='table'
) }}
WITH wallet_history AS (
    SELECT * FROM {{ ref('stg_wallet_tokens_test_data') }}
),

token_metadata AS (
    SELECT * FROM {{ ref('stg_tokens_metadata_test_data') }}
),


popular_tokens AS (
    SELECT
        wh.token_address,
        COUNT(DISTINCT wh.wallet_address) AS wallet_count
    FROM
        wallet_history AS wh
    GROUP BY
        wh.token_address
    HAVING
        COUNT(DISTINCT wh.wallet_address) >= 500
),

joined_and_filtered AS (
    SELECT
        wh.wallet_address,
        wh.token_address,
        wh.transaction_count,
        wh.asset_amount,
        wh.first_acquired_timestamp,
        wh.last_acquired_timestamp,
        tm.token_metadata_sk
    FROM
        wallet_history AS wh
    INNER JOIN
        token_metadata AS tm
        ON wh.token_address = tm.token_address
    INNER JOIN
        popular_tokens AS pt 
        ON wh.token_address = pt.token_address
)

SELECT * FROM joined_and_filtered