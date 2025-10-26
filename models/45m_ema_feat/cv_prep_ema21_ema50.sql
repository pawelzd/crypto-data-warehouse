-- models/cv_prices_sessionized.sql
{{ config(materialized='table') }}

{% set mc_threshold   = 100000000 %}  -- 100M
{% set window_seconds = 172800 %}     -- 2 days
{# Optional: cap how far we forward-fill within [min_ts, max_ts]. Set to NULL for unlimited. #}
{% set max_fill_hours = None %}       {# e.g., 48 to limit to 48h; keep None to disable #}

-- 1) Raw bases (unchanged)
WITH base_sol AS (
  SELECT
    tp.chain,
    tp.token_chain_id,
    tp.token_address,
    tp.price_timestamp,
    SAFE_CAST(tp.close  AS FLOAT64) AS price_usd,
    SAFE_CAST(tp.volume AS FLOAT64) AS volume,
    (utb.totalSupply * tp.close)    AS mktcap
  FROM {{ ref('token_ohlcv_view') }} AS tp
  JOIN {{ source('core', 'token_metadata_jup_tmp') }} AS utb
    ON utb.id = tp.token_address
  WHERE tp.chain = 'sol'
    AND (utb.totalSupply * tp.close) >= {{ mc_threshold }}
),
base_other AS (
  SELECT
    tp.chain,
    tp.token_chain_id,
    tp.token_address,
    tp.price_timestamp,
    SAFE_CAST(tp.close  AS FLOAT64) AS price_usd,
    SAFE_CAST(tp.volume AS FLOAT64) AS volume,
    (utb.total_supply * tp.close)   AS mktcap
  FROM {{ ref('token_ohlcv_view') }} AS tp
  JOIN {{ source('core', 'tmp_birdeye_static_data') }} AS utb
    ON utb.token_chain_id = tp.token_chain_id
  WHERE tp.chain <> 'sol'
),
base_all AS (
  SELECT * FROM base_sol
  UNION ALL
  SELECT * FROM base_other
)

SELECT 
    *
FROM base_all
