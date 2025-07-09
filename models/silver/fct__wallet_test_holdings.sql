WITH holdings AS (
  SELECT * FROM {{ ref('fct_wallet_holdings') }}
),
prices AS (
  SELECT * FROM {{ ref('dim_token_prices') }}
),

-- Join each holding to all prices for that token that occurred on or before the holding's load_date
joined_with_all_prior_prices AS (
  SELECT
    holdings.load_date,
    holdings.wallet_address,
    holdings.token_address,
    holdings.asset_amount,
    prices.price_timestamp,
    prices.price_usd,
    
    -- Rank the prices for each holding, with 1 being the most recent
    ROW_NUMBER() OVER (
      PARTITION BY holdings.wallet_address, holdings.token_address, holdings.load_date
      ORDER BY prices.price_timestamp DESC
    ) AS price_rank
  FROM holdings
  LEFT JOIN prices
    ON holdings.token_address = prices.token_address
    AND holdings.load_date >= CAST(prices.price_timestamp AS DATE)
)

-- Select only the most recent price (rank = 1) for each holding
SELECT
  load_date,
  wallet_address,
  token_address,
  asset_amount,
  price_usd,
  (asset_amount * price_usd) AS value_usd_at_snapshot
FROM joined_with_all_prior_prices
WHERE price_rank = 1