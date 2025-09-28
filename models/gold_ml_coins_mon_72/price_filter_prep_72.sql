{{ config(
    schema='gold_ml_coins_mon_72',
    materialized='table'
) }}

{#--- tweakable params ---#}
{% set mc_threshold = 1000000 %}
{% set window_seconds = 172800 %} {# 2 days in seconds #}

WITH base AS (
  SELECT
    tp.token_address,
    tp.price_timestamp,
    tp.price_usd,
    (tp.price_usd * tmm.total_supply) AS mktcap
  FROM {{ ref('fct__token_prices') }} AS tp
  INNER JOIN {{ source('gold', 'unique_tokens_base') }} AS utb
    ON utb.token_address = tp.token_address
  INNER JOIN {{ source('silver', 'fct__token_market_metadata') }} AS tmm
    ON tp.token_address = tmm.token_address
  WHERE utb.max_mc > {{ mc_threshold }}
),
with_flags AS (
  SELECT
    token_address,
    price_timestamp,
    price_usd,
    mktcap,
    CASE WHEN mktcap >= {{ mc_threshold }} THEN 1 ELSE 0 END AS flag_above
  FROM base
),
with_monitoring AS (
  SELECT
    token_address,
    price_timestamp,
    price_usd,
    mktcap,
    flag_above,
    CASE
      WHEN
        MAX(flag_above) OVER (
          PARTITION BY token_address
          ORDER BY UNIX_SECONDS(price_timestamp)
          RANGE BETWEEN {{ window_seconds }} PRECEDING AND CURRENT ROW
        ) = 1
      THEN TRUE ELSE FALSE
    END AS is_monitored
  FROM with_flags
),
with_sessions AS (
  SELECT
    token_address,
    price_timestamp,
    price_usd,
    mktcap,
    is_monitored,
    CASE
      WHEN is_monitored = TRUE
           AND COALESCE(LAG(is_monitored) OVER (
                 PARTITION BY token_address ORDER BY price_timestamp
               ), FALSE) = FALSE
      THEN 1 ELSE 0
    END AS session_start_flag
  FROM with_monitoring
),
sessionized AS (
  SELECT
    token_address,
    price_timestamp,
    price_usd,
    mktcap,
    is_monitored,
    CASE
      WHEN is_monitored THEN
        SUM(session_start_flag) OVER (
          PARTITION BY token_address
          ORDER BY price_timestamp
          ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        )
      ELSE NULL
    END AS monitoring_session_id
  FROM with_sessions
)
SELECT
  token_address,
  price_timestamp,
  price_usd,
  mktcap,
  is_monitored,
  monitoring_session_id
FROM sessionized
ORDER BY token_address, price_timestamp
