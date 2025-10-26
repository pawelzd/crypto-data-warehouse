-- models/45m_ema_feat/cv_ema21_ema50_labels.sql
{{ config(materialized='table') }}

{% set alpha21 = 2.0 / (21 + 1) %}
{% set alpha50 = 2.0 / (50 + 1) %}

WITH RECURSIVE
base AS (
  SELECT
    chain,
    token_chain_id,
    token_address,
    TIMESTAMP_TRUNC(price_timestamp, HOUR) AS ts_hour,
    AVG(price_usd) AS price,               -- hourly bar
    ANY_VALUE(mktcap)  AS mktcap,          -- or AVG/SUM if you prefer
    SUM(volume)        AS volume,
    MAX(is_monitored)  AS is_monitored,    -- if any trade is monitored, mark hour monitored
    MAX(monitoring_session_id) AS monitoring_session_id
  FROM {{ ref('cv_prep_ema21_ema50') }}
  GROUP BY 1,2,3,4
),

indexed AS (
  SELECT
    b.*,
    ROW_NUMBER() OVER (
      PARTITION BY chain, token_chain_id
      ORDER BY ts_hour
    ) AS rn
  FROM base b
),

-- EMA recursion (seed with first price -> pandas adjust=False)
ema AS (
  -- seed
  SELECT
    i.chain,
    i.token_chain_id,
    i.token_address,
    i.ts_hour,
    i.price,
    i.volume,
    i.mktcap,
    i.is_monitored,
    i.monitoring_session_id,
    i.rn,
    CAST(i.price AS FLOAT64) AS ema_21,
    CAST(i.price AS FLOAT64) AS ema_50
  FROM indexed i
  WHERE i.rn = 1

  UNION ALL

  -- step
  SELECT
    n.chain,
    n.token_chain_id,
    n.token_address,
    n.ts_hour,
    n.price,
    n.volume,
    n.mktcap,
    n.is_monitored,
    n.monitoring_session_id,
    n.rn,
    (p.ema_21 * (1 - {{ alpha21 }}) + n.price * {{ alpha21 }}) AS ema_21,
    (p.ema_50 * (1 - {{ alpha50 }}) + n.price * {{ alpha50 }}) AS ema_50
  FROM ema p
  JOIN indexed n
    ON n.chain          = p.chain
   AND n.token_chain_id = p.token_chain_id
   AND n.rn             = p.rn + 1
),

final AS (
  SELECT
    chain,
    token_chain_id,
    token_address,
    ts_hour,
    price,
    volume,
    mktcap,
    is_monitored,
    monitoring_session_id,
    rn,
    ema_21,
    ema_50,
    LAG(ema_21) OVER (PARTITION BY chain, token_chain_id ORDER BY ts_hour) AS ema_21_prev,
    LAG(ema_50) OVER (PARTITION BY chain, token_chain_id ORDER BY ts_hour) AS ema_50_prev
  FROM ema
)

SELECT
  chain,
  token_chain_id,
  token_address,
  ts_hour,
  price,
  volume,
  mktcap,
  is_monitored,
  monitoring_session_id,
  ema_21,
  ema_50,
  ema_21_prev,
  ema_50_prev,
  CASE
    WHEN rn >= 50
     AND (ema_21_prev - ema_50_prev) <=  1e-9
     AND (ema_21      - ema_50     ) >   1e-9
    THEN 1 ELSE 0
  END AS label_entry,
  CASE
    WHEN rn >= 50
     AND (ema_21_prev - ema_50_prev) >= -1e-9
     AND (ema_21      - ema_50     ) <  -1e-9
    THEN 1 ELSE 0
  END AS label_close
FROM final
ORDER BY chain, token_chain_id, ts_hour
