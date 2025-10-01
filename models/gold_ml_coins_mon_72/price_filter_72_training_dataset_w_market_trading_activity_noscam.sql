{{ config(
    schema='gold_ml_coins_mon_72',
    materialized='table'
) }}


SELECT
  pft.* 
FROM {{ ref('price_filter_72_training_dataset_w_market_trading_activity') }} AS pft
INNER JOIN {{ source('silver', 'dim__token_score') }} AS ts
  ON ts.token_address = pft.token_address  -- cross join to get BTC features for all rows (no filtering)
WHERE ts.organicScoreLabel = 'medium' OR ts.organicScoreLabel = 'high'
