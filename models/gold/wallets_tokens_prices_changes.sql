WITH holdings AS (
  SELECT
    *
  FROM
    {{ ref('fct__wallet_holdings') }}
),

prices AS (
  SELECT
    *
  FROM
    {{ ref('fct__token_prices') }}
),

cleaned_wallets AS (
  SELECT 
    h.wallet_address,
    h.token_address,
    h.first_acquired_timestamp,
    h.transaction_count,
    h.asset_amount
  FROM 
    holdings AS h 
),

cleaned_prices AS (
  SELECT 
    p.token_address,
    p.price_timestamp,
    p.price_usd 
  FROM 
    prices AS p
),

ranked_acquisition_prices AS (
    SELECT
        w.wallet_address,
        w.token_address,
        p.price_usd AS price_when_acquired,
        ROW_NUMBER() OVER(
            PARTITION BY w.wallet_address, w.token_address
            ORDER BY ABS(TIMESTAMP_DIFF(w.first_acquired_timestamp, p.price_timestamp, HOUR)) ASC
        ) AS rn
    FROM cleaned_wallets AS w
    INNER JOIN cleaned_prices AS p
        ON w.token_address = p.token_address
),

-- Step 2: Use a standard WHERE clause to filter for the top-ranked row
acquisition_prices AS (
    SELECT
        wallet_address,
        token_address,
        price_when_acquired
    FROM ranked_acquisition_prices
    WHERE rn = 1
)


SELECT 
    w.wallet_address,
    p.token_address,
    w.first_acquired_timestamp,
    p.price_timestamp,
    -- DATE_DIFF(CAST(p.price_timestamp AS DATE), CAST(w.first_acquired_timestamp AS DATE), DAY) AS days_since_acquired, 
    -- TIMESTAMP_DIFF(p.price_timestamp, w.first_acquired_timestamp, HOUR) AS hours_since_acquired, 
    w.transaction_count,
    w.asset_amount,
    p.price_usd,
    ap.price_when_acquired, 
    (p.price_usd - ap.price_when_acquired) AS price_change

FROM
    cleaned_wallets AS w
INNER JOIN
    cleaned_prices AS p ON w.token_address = p.token_address
LEFT JOIN
    acquisition_prices AS ap
    ON w.wallet_address = ap.wallet_address AND w.token_address = ap.token_address
