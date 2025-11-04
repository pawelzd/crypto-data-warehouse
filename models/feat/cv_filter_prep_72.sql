
{#--- tweakable params ---#}
{% set mc_threshold = 1000000 %}
{% set window_seconds = 172800 %} {# 2 days in seconds #}

-- WITH base AS (
--   SELECT
--     tp.token_address,
--     tp.price_timestamp,
--     tp.price_usd,
--     tp.volume,
--     utb.mktcap AS mktcap
--   FROM {{ ref('token_cv') }} AS tp
--   INNER JOIN {{ ref('unique_token_mc') }} AS utb
--     ON utb.token_address = tp.token_address
--     AND utb.price_timestamp = tp.price_timestamp
--   WHERE utb.max_mc > {{ mc_threshold }}

-- ),


WITH base AS (
  SELECT
    tp.token_address,
    tp.price_timestamp,
    tp.price_usd,
    tp.volume,
    tp.mktcap AS mktcap
  FROM {{ ref('cv_filter_prep_72_view') }} AS tp
  WHERE (tp.mktcap >= {{ mc_threshold }} AND  tp.chain = 'sol')
     OR (tp.mktcap >= 45000000 AND  tp.chain <> 'sol')

),
with_flags AS (
  SELECT
    token_address,
    price_timestamp,
    price_usd,
    volume,
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
    volume,
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
    volume,
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
    volume,
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
SELECT DISTINCT
  token_address,
  price_timestamp,
  price_usd,
  mktcap,
  volume,
  is_monitored,
  monitoring_session_id
FROM sessionized
ORDER BY token_address, price_timestamp
