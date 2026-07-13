WITH output_tokens AS (
  SELECT DISTINCT
    'live' AS relation_name,
    token_address
  FROM {{ ref('rl_prod_inference_features_v') }}

  UNION DISTINCT

  SELECT DISTINCT
    'history' AS relation_name,
    token_address
  FROM {{ ref('rl_prod_inference_features_history_v') }}
),

scam_tokens AS (
  SELECT DISTINCT
    token_address
  FROM {{ ref('scam_h_union') }}
  WHERE chain = 'sol'
)

SELECT
  o.relation_name,
  o.token_address
FROM output_tokens o
INNER JOIN scam_tokens s
  USING (token_address)
