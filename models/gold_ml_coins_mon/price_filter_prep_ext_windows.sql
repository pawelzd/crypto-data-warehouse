{{ config(
    materialized='table'
) }}

{#-------------------- tweakable params --------------------#}
{% set lookback_days  = 7 %}
{% set lookahead_days = 7 %}

{#----------------------------------------------------------#}
-- If your model name is exactly "price_filter_prep", use ref('price_filter_prep').
-- If dbt renames with folder prefixing, change to ref('gold_ml_coins_mon__price_filter_prep').
WITH prep AS (
  SELECT
    token_address,
    price_timestamp,
    price_usd,
    mktcap,
    is_monitored,
    monitoring_session_id
  FROM {{ ref('price_filter_prep') }}
),

-- Core monitored session boundaries (min/max of timestamps within each monitoring session)
session_bounds AS (
  SELECT
    token_address,
    monitoring_session_id,
    MIN(price_timestamp) AS session_start,
    MAX(price_timestamp) AS session_end
  FROM prep
  WHERE monitoring_session_id IS NOT NULL
  GROUP BY token_address, monitoring_session_id
),

-- Extend each session by ± lookback/lookahead days
extended_bounds AS (
  SELECT
    token_address,
    monitoring_session_id,
    TIMESTAMP_SUB(session_start, INTERVAL {{ lookback_days }} DAY)  AS extended_start,
    session_start,
    session_end,
    TIMESTAMP_ADD(session_end,   INTERVAL {{ lookahead_days }} DAY) AS extended_end
  FROM session_bounds
),

-- Join extended windows back to the full price stream to pull all rows covered by any extension
candidate_rows AS (
  SELECT
    p.token_address,
    p.price_timestamp,
    p.price_usd,
    p.mktcap,

    -- Bring session info from the extended window it falls into
    e.monitoring_session_id,
    e.extended_start,
    e.session_start,
    e.session_end,
    e.extended_end,

    -- Flags to help downstream feature/label generation
    -- Pre-extension: rows in [extended_start, session_start)
    CASE
      WHEN p.price_timestamp >= e.extended_start
       AND p.price_timestamp <  e.session_start
      THEN TRUE ELSE FALSE END AS in_pre_extension,

    -- Core monitoring: rows in [session_start, session_end]
    CASE
      WHEN p.price_timestamp >= e.session_start
       AND p.price_timestamp <= e.session_end
      THEN TRUE ELSE FALSE END AS in_core_monitoring,

    -- Post-extension: rows in (session_end, extended_end]
    CASE
      WHEN p.price_timestamp >  e.session_end
       AND p.price_timestamp <= e.extended_end
      THEN TRUE ELSE FALSE END AS in_post_extension
  FROM prep p
  JOIN extended_bounds e
    ON p.token_address   = e.token_address
   AND p.price_timestamp BETWEEN e.extended_start AND e.extended_end
),

-- If extended windows overlap (rare but possible), a single price row might match multiple sessions.
-- Deduplicate by assigning each (token, timestamp) to the "closest" core session boundary.
-- Distance = seconds to nearest point of the core window [session_start, session_end].
dedup_base AS (
  SELECT
    *,
    CASE
      WHEN price_timestamp < session_start
        THEN TIMESTAMP_DIFF(session_start, price_timestamp, SECOND)
      WHEN price_timestamp > session_end
        THEN TIMESTAMP_DIFF(price_timestamp, session_end, SECOND)
      ELSE 0
    END AS distance_to_core
  FROM candidate_rows
),

-- Now you can safely reference distance_to_core in the window ORDER BY
deduped AS (
  SELECT
    *,
    ROW_NUMBER() OVER (
      PARTITION BY token_address, price_timestamp
      ORDER BY
        CASE
          WHEN in_core_monitoring THEN 0
          WHEN in_pre_extension OR in_post_extension THEN 1
          ELSE 2
        END,
        distance_to_core ASC,
        monitoring_session_id ASC
    ) AS rn
  FROM dedup_base
)

SELECT
  token_address,
  price_timestamp,
  price_usd,
  mktcap,

  -- Final session mapping (unique per token/timestamp after dedupe)
  monitoring_session_id,
  session_start,
  session_end,
  extended_start,
  extended_end,

  -- Convenience phase flags
  in_pre_extension,
  in_core_monitoring,
  in_post_extension
FROM deduped
WHERE rn = 1
ORDER BY token_address, price_timestamp
