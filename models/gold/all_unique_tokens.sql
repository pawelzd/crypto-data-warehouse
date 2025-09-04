WITH holdings AS (
  SELECT
    *
  FROM
    {{ ref('fct__wallet_holdings') }}
),

tokens_over_1k_wallets AS (
  SELECT
    token_address,
    COUNT(DISTINCT wallet_address) AS number_of_wallets
  FROM
    holdings
  GROUP BY
    token_address
  HAVING
    number_of_wallets  >= 1100
),

tokens_active_last_3d AS (
  SELECT
    token_address
  FROM
    holdings
  WHERE
    TIMESTAMP_DIFF(TIMESTAMP('2025-07-19 15:43:36 UTC'), last_acquired_timestamp, DAY) <= 3
  GROUP BY
    token_address
)

SELECT
  t1.token_address, 
  t1.number_of_wallets
FROM
  tokens_over_1k_wallets t1
INNER JOIN
  tokens_active_last_3d t2
ON
  t1.token_address = t2.token_address
