{{ config(materialized='view') }}

-- Union of all hourly scam pattern models
-- (everything except scam_h_features)

WITH decay AS (
  SELECT chain, token_address
  FROM {{ ref('scam_h_decay') }}
  WHERE pattern_decay_to_dust = 1
),

flash AS (
  SELECT chain, token_address
  FROM {{ ref('scam_h_flash') }}
  WHERE pattern_flash_crash = 1
),

frozen AS (
  SELECT chain, token_address
  FROM {{ ref('scam_h_frozen') }}
  WHERE pattern_frozen_price = 1
),

park AS (
  SELECT chain, token_address
  FROM {{ ref('scam_h_park') }}
  WHERE pattern_pump_and_park = 1
),

pump AS (
  SELECT chain, token_address
  FROM {{ ref('scam_h_pump') }}
  WHERE pattern_pump_cliff = 1
),

side AS (
  -- “one-sided / liquidity trap”
  SELECT chain, token_address
  FROM {{ ref('scam_h_side') }}
  WHERE pattern_liquidity_trap = 1
),

thin AS (
  -- “thin liquidity + high volatility”
  SELECT chain, token_address
  FROM {{ ref('scam_h_thin') }}
  WHERE pattern_low_vol_high_volatility = 1
),

wash AS (
  SELECT chain, token_address
  FROM {{ ref('scam_h_wash') }}
  WHERE pattern_wash_trading = 1
),

wicks AS (
  SELECT chain, token_address
  FROM {{ ref('scam_h_wicks') }}
  WHERE pattern_extreme_wick = 1
)

SELECT
  chain,
  token_address,
  'decay_to_dust' AS scam_pattern
FROM decay

UNION ALL
SELECT
  chain,
  token_address,
  'flash_crash' AS scam_pattern
FROM flash

UNION ALL
SELECT
  chain,
  token_address,
  'frozen_price' AS scam_pattern
FROM frozen

UNION ALL
SELECT
  chain,
  token_address,
  'pump_and_park' AS scam_pattern
FROM park

UNION ALL
SELECT
  chain,
  token_address,
  'pump_cliff_rug' AS scam_pattern
FROM pump

UNION ALL
SELECT
  chain,
  token_address,
  'liquidity_trap_one_sided' AS scam_pattern
FROM side

UNION ALL
SELECT
  chain,
  token_address,
  'thin_liquidity_high_volatility' AS scam_pattern
FROM thin

UNION ALL
SELECT
  chain,
  token_address,
  'wash_trading' AS scam_pattern
FROM wash

UNION ALL
SELECT
  chain,
  token_address,
  'extreme_wicks' AS scam_pattern
FROM wicks
