{{ config(
    materialized='incremental',
    unique_key='exchange_symbol',
    incremental_strategy='merge'
) }}

WITH new_flags AS (
  SELECT
    exchange,
    symbol,
    MAX(ts) AS last_flagged_at,
    MAX(live_scam_score) AS max_score,
    MAX_BY(processed_at, ts) AS last_processed_at
  FROM {{ ref('scam_h_features') }}
  WHERE live_scam_score > 0
  GROUP BY 1,2
)

SELECT
  exchange,
  symbol,
  last_flagged_at,
  max_score,
  last_processed_at,
  CURRENT_TIMESTAMP() AS updated_at
FROM new_flags
{% if is_incremental() %}
WHERE last_flagged_at > (SELECT MAX(last_flagged_at) FROM {{ this }})
{% endif %}
