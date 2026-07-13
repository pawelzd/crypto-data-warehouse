SELECT
  token_address,
  price_timestamp
FROM {{ ref('rl_prod_inference_features_v') }}
WHERE NOT in_universe_pit
