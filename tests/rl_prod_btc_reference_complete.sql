SELECT token_address, price_timestamp
FROM {{ ref('rl_prod_inference_features_history_v') }}
WHERE rel_excess_vs_btc_7d IS NULL
   OR rel_excess_vs_btc_24h IS NULL
