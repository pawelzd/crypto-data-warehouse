{{ config(
    schema='core',
    materialized='table'
) }}


WITH
-- 1) Normalize to one row per token per hour (dedupe within an hour)
bars AS (
  SELECT DISTINCT
    token_address,
    chain,
    TIMESTAMP_TRUNC(price_timestamp, HOUR) AS ts_hour
  FROM {{ ref('token_ohlcv') }}
),

-- 2) Look at consecutive hours and measure the gap between them
with_prev AS (
  SELECT
    token_address,
    chain,
    ts_hour AS next_ts,
    LAG(ts_hour) OVER (
      PARTITION BY token_address, chain
      ORDER BY ts_hour
    ) AS prev_ts
  FROM bars
),

-- 3) Keep only gaps > 1 hour and compute size
gaps AS (
  SELECT
    token_address,
    chain,
    prev_ts,
    next_ts,
    TIMESTAMP_DIFF(next_ts, prev_ts, HOUR) - 1 AS gap_hours,
    GENERATE_TIMESTAMP_ARRAY(
      TIMESTAMP_ADD(prev_ts, INTERVAL 1 HOUR),
      TIMESTAMP_ADD(next_ts, INTERVAL -1 HOUR),
      INTERVAL 1 HOUR
    ) AS missing_hours
  FROM with_prev
  WHERE prev_ts IS NOT NULL
    AND TIMESTAMP_DIFF(next_ts, prev_ts, HOUR) > 1
)

-- 4) Show the biggest gaps (one row per gap)
SELECT
  token_address,
  chain,
  prev_ts AS gap_start_observed,   -- last hour you DO have before the gap
  next_ts AS gap_end_observed,     -- first hour you DO have after the gap
  gap_hours,                       -- number of missing hours inside
  ARRAY_LENGTH(missing_hours) AS missing_count
  -- , missing_hours                -- uncomment if you want the array
FROM gaps
ORDER BY gap_hours DESC, token_address, chain
