-- Cross-sectional features must be calculated over PIT members only, even
-- though the history relation retains nonmember rows as warm-up/death context.
WITH per_hour AS (
  SELECT
    price_timestamp,
    COUNT(DISTINCT IF(in_universe_pit, token_address, NULL)) AS actual_n,
    MIN(univ_n_active) AS min_feature_n,
    MAX(univ_n_active) AS max_feature_n
  FROM {{ ref('rl_prod_inference_features_history_v') }}
  GROUP BY price_timestamp
  HAVING actual_n > 0
)
SELECT p.*
FROM per_hour p
WHERE p.min_feature_n != p.max_feature_n
   OR p.actual_n != p.max_feature_n
   OR p.max_feature_n < 1
