{#--- tweakable params ---#}
{% set mc_threshold = 20000000 %}
{% set tz_name = 'Europe/Paris' %}

WITH base AS (
  SELECT
    tp.token_address,
    tp.price_timestamp,     -- TIMESTAMP (UTC)
    tp.price_usd,
    tp.volume,
    utb.mktcap AS mktcap
  FROM {{ ref('token_cv') }} AS tp
  INNER JOIN {{ ref('unique_token_mc') }} AS utb
    ON utb.token_address = tp.token_address
   AND utb.price_timestamp = tp.price_timestamp
  WHERE utb.max_mc > {{ mc_threshold }}
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

-- Identify runs and where we first DROP below the threshold (transition 1 -> 0)
with_transitions AS (
  SELECT
    token_address,
    price_timestamp,
    price_usd,
    mktcap,
    volume,
    flag_above,
    LAG(flag_above) OVER (PARTITION BY token_address ORDER BY price_timestamp) AS prev_flag,
    CASE
      WHEN flag_above = 0
       AND COALESCE(LAG(flag_above) OVER (PARTITION BY token_address ORDER BY price_timestamp), 0) = 1
      THEN 1 ELSE 0
    END AS is_drop_start
  FROM with_flags
),

-- Number each consecutive "below-threshold" run so we can find the drop start timestamp
zero_runs AS (
  SELECT
    token_address,
    price_timestamp,
    price_usd,
    mktcap,
    volume,
    flag_above,
    prev_flag,
    is_drop_start,
    /* zero_run_id increments only when a new below-threshold run starts */
    SUM(CASE WHEN flag_above = 0 AND is_drop_start = 1 THEN 1 ELSE 0 END)
      OVER (PARTITION BY token_address ORDER BY price_timestamp
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS zero_run_id
  FROM with_transitions
),

-- For rows in a below-threshold run, get the run's first timestamp (drop_ts)
with_drop_ts AS (
  SELECT
    token_address,
    price_timestamp,
    price_usd,
    mktcap,
    volume,
    flag_above,
    zero_run_id,
    /* If we're below threshold, this is the start time of THIS run; else NULL */
    CASE
      WHEN flag_above = 0 AND zero_run_id > 0 THEN
        MIN(CASE WHEN is_drop_start = 1 THEN price_timestamp END)
          OVER (PARTITION BY token_address, zero_run_id)
      ELSE NULL
    END AS drop_ts
  FROM zero_runs
),

-- Compute the next local 07:00 after drop_ts in the desired timezone
with_cutoff AS (
  SELECT
    token_address,
    price_timestamp,
    price_usd,
    mktcap,
    volume,
    flag_above,
    drop_ts,
    CASE
      WHEN drop_ts IS NULL THEN NULL
      ELSE
        -- Convert drop_ts to local datetime
        TIMESTAMP(
          DATETIME(
            CASE
              WHEN TIME(DATETIME(drop_ts, '{{ tz_name }}')) < TIME '07:00:00'
                THEN DATE(DATETIME(drop_ts, '{{ tz_name }}'))
              ELSE DATE_ADD(DATE(DATETIME(drop_ts, '{{ tz_name }}')), INTERVAL 1 DAY)
            END,
            TIME '07:00:00'
          ),
          '{{ tz_name }}'
        )
    END AS next_7am_after_drop
  FROM with_drop_ts
),

-- is_monitored:
--   - TRUE while at/above threshold
--   - If below, TRUE only until the next local 07:00 after the first drop in that below-threshold run
with_monitoring AS (
  SELECT
    token_address,
    price_timestamp,
    price_usd,
    mktcap,
    volume,
    CASE
      WHEN flag_above = 1 THEN TRUE
      WHEN flag_above = 0 AND next_7am_after_drop IS NOT NULL
           AND price_timestamp < next_7am_after_drop THEN TRUE
      ELSE FALSE
    END AS is_monitored
  FROM with_cutoff
),

-- Sessionize contiguous monitored periods per token
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
