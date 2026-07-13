{{ config(materialized='view') }}

-- This relation is the exclusion contract consumed by training and serving.
-- Death/event signatures remain available in scam_full_history_detections for
-- audit, but are deliberately not lifetime exclusions: consuming them here
-- would remove the pre-collapse history and introduce lookahead/survivorship
-- bias. A lifetime exclusion also requires evidence spanning more than 90
-- days; one bad episode can populate up to three overlapping detector windows.

WITH hardened_lifetime AS (
  SELECT
    chain,
    token_address,
    scam_pattern
  FROM {{ source('scam_detection_artifacts', 'scam_full_history_detections') }}
  WHERE scam_pattern IN (
    'stablecoin',
    'liquidity_trap_one_sided',
    'wash_trading',
    'extreme_wicks',
    'extreme_micro',
    'thin_liquidity_high_volatility',
    'frozen_price'
  )
  GROUP BY chain, token_address, scam_pattern
  HAVING DATE_DIFF(MAX(window_start), MIN(window_start), DAY) > 90
),

manual AS (
  SELECT
    chain,
    token_address,
    CONCAT('manual:', notes) AS scam_pattern
  FROM {{ ref('scam_manual') }}
)

SELECT * FROM hardened_lifetime
UNION ALL
SELECT * FROM manual
