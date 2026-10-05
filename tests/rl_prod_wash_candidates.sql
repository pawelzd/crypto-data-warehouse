{{ config(severity='warn') }}
-- NEW wash-trading candidates among the latest week's members, from Birdeye's
-- market snapshot (raw_birdeye_market_data, refreshed weekly): fewer than 100
-- wallets behind >= 250k USD of 24h volume, or fewer than 2000 holders behind
-- >= 50M USD of market cap, excluding tokens already curated in
-- wash_traded_addresses and off-class assets (pegs/wrappers/LSTs are handled by
-- their own detectors and trip this rule legitimately: LBTC, TUSD, XAUt).
-- WARN, not error: a hit is a review item — add it to wash_traded_addresses
-- (forward-only) if it is wash-traded, or note why not.
WITH latest_week AS (
  SELECT MAX(week_start) AS wk FROM {{ ref('rl_prod_universe_membership_v1_state') }}
),
members AS (
  SELECT m.token_address
  FROM {{ ref('rl_prod_universe_membership_v1_state') }} m, latest_week l
  WHERE m.in_universe_pit AND m.week_start = l.wk
),
snap AS (
  SELECT address, symbol, volume_24h_usd AS vol, unique_wallet_24h AS w, holder AS h, market_cap AS mc
  FROM {{ source('raw', 'raw_birdeye_market_data') }}
  QUALIFY ROW_NUMBER() OVER (PARTITION BY address ORDER BY last_trade_unix_time DESC) = 1
)
SELECT s.address AS token_address, s.symbol, s.vol, s.w AS wallets, s.h AS holders, s.mc AS market_cap
FROM snap s JOIN members m ON m.token_address = s.address
WHERE ((s.w < 100 AND s.vol >= 250000) OR (s.h < 2000 AND s.mc >= 5e7))
  AND s.address NOT IN (
    'D4BPL1zvhhJbxUdgi2qVUtjx4jeQWyUr2PAUjKc9rN5x', '7pKXpFsnZS5BB4Eydk3uZ84FeKDSvkv1z4Hv5ayQ28RV',
    'C8fU5GdfAt5mnw2RK7HE6XJGFNxHpaskZMkXxdm88888', '2bpT3ksMdwdZ6DuHyq3FDUr7HDwvZ5DRZoT1fUPALJaH')
  AND UPPER(s.symbol) NOT IN ('XSHIN','LBTC','WBTC','CBBTC','ZBTC','WETH','XAUT','TUSD','USDC','USDT','PYUSD','USDS')
  AND NOT ENDS_WITH(UPPER(s.symbol), 'SOL')
