{{ config(
    materialized='incremental',
    partition_by={'field': 'current_dt', 'data_type': 'timestamp'},
    cluster_by=['token_address'],
    unique_key=['current_trade_id'],
    on_schema_change='sync_all_columns'
) }}

{% set BUY_PROB = var('buy_prob_threshold', 0.8347) %}

-- 1) Model-1 entries (buy decisions) over time
WITH entries AS (
  SELECT
    GENERATE_UUID() AS trade_id,
    r.token_address,
    r.decision_ts,
    r.price AS entry_price,
    -- handy prob on row
    (SELECT p.prob FROM UNNEST(r.predicted_label_profit20_before_loss25_probs) p WHERE p.label = 1) AS prob_buy
  FROM {{ source('eval', 'forest_72_gain20_loss25_og_duplicate_prod_results') }} r
  WHERE (SELECT p.prob FROM UNNEST(r.predicted_label_profit20_before_loss25_probs) p WHERE p.label = 1) >= {{ BUY_PROB }}

  {% if is_incremental() %}
    AND r.decision_ts > (
      SELECT COALESCE(MAX(current_dt), TIMESTAMP '1970-01-01 00:00:00+00')
      FROM {{ this }}
    )
  {% endif %}
),

-- 2) For each CURRENT entry, find PRIOR entries (same token) in last 7d (we’ll aggregate by multiple windows)
prior_entries AS (
  SELECT
    c.trade_id     AS current_trade_id,
    c.token_address,
    c.decision_ts  AS current_dt,
    c.entry_price  AS current_entry_price,
    c.prob_buy     AS current_prob_buy,
    p.trade_id     AS prior_trade_id,
    p.decision_ts  AS prior_dt,
    p.entry_price  AS prior_entry_price
  FROM entries c
  JOIN entries p
    ON p.token_address = c.token_address
   AND p.decision_ts  < c.decision_ts
   AND p.decision_ts  >= TIMESTAMP_SUB(c.decision_ts, INTERVAL 7 DAY)
),

-- 3) Price path for each PRIOR trade, truncated at current decision (AS-OF safety)
prior_paths AS (
  SELECT
    pe.current_trade_id, pe.token_address, pe.current_dt,
    pe.prior_trade_id,   pe.prior_dt,      pe.prior_entry_price,
    h.datetime AS hour_ts,
    h.price    AS px_now,
    SAFE_DIVIDE(h.price, pe.prior_entry_price) - 1 AS ret
  FROM prior_entries pe
  JOIN {{ ref('cv_prod_filled_hours') }} h
    ON h.address  = pe.token_address
   AND h.datetime > pe.prior_dt
   AND h.datetime <= pe.current_dt
),

-- 4) First time each prior trade hits TP/SL before current_dt, plus last seen
flags AS (
  SELECT
    pp.*,
    MIN(IF(ret >=  0.20, hour_ts, NULL)) OVER (PARTITION BY current_trade_id, prior_trade_id) AS tp_ts,
    MIN(IF(ret <= -0.25, hour_ts, NULL)) OVER (PARTITION BY current_trade_id, prior_trade_id) AS sl_ts,
    MAX(hour_ts)                             OVER (PARTITION BY current_trade_id, prior_trade_id) AS last_seen_ts
  FROM prior_paths pp
),

-- 5) Status of prior trades as-of current_dt
status_asof AS (
  SELECT DISTINCT
    f.current_trade_id, f.token_address, f.current_dt,
    f.prior_trade_id,   f.prior_dt,
    CASE
      WHEN f.tp_ts IS NULL AND f.sl_ts IS NULL THEN NULL
      WHEN f.sl_ts IS NULL THEN f.tp_ts
      WHEN f.tp_ts IS NULL THEN f.sl_ts
      WHEN f.tp_ts <= f.sl_ts THEN f.tp_ts
      ELSE f.sl_ts
    END AS close_ts,
    f.tp_ts, f.sl_ts, f.last_seen_ts,
    CASE
      WHEN (CASE
              WHEN f.tp_ts IS NULL AND f.sl_ts IS NULL THEN NULL
              WHEN f.sl_ts IS NULL THEN f.tp_ts
              WHEN f.tp_ts IS NULL THEN f.sl_ts
              WHEN f.tp_ts <= f.sl_ts THEN f.tp_ts
              ELSE f.sl_ts
            END) IS NULL THEN 'OPEN'
      WHEN (CASE
              WHEN f.tp_ts IS NULL AND f.sl_ts IS NULL THEN NULL
              WHEN f.sl_ts IS NULL THEN f.tp_ts
              WHEN f.tp_ts IS NULL THEN f.sl_ts
              WHEN f.tp_ts <= f.sl_ts THEN f.tp_ts
              ELSE f.sl_ts
            END) = f.tp_ts THEN 'TP20'
      ELSE 'SL25'
    END AS prior_status_asof,
    TIMESTAMP_DIFF(f.current_dt, f.prior_dt, HOUR) AS age_h
  FROM flags f
),

-- 6) Aggregate meta-features across multiple windows
agg AS (
  SELECT
    s.current_trade_id,
    s.token_address,
    s.current_dt,

    COUNTIF(age_h <= 12)                                AS buys_12h,
    COUNTIF(age_h <= 12 AND prior_status_asof='TP20')   AS wins_12h,
    COUNTIF(age_h <= 12 AND prior_status_asof='SL25')   AS losses_12h,
    COUNTIF(age_h <= 12 AND prior_status_asof='OPEN')   AS open_12h,

    COUNTIF(age_h <= 24)                                AS buys_24h,
    COUNTIF(age_h <= 24 AND prior_status_asof='TP20')   AS wins_24h,
    COUNTIF(age_h <= 24 AND prior_status_asof='SL25')   AS losses_24h,
    COUNTIF(age_h <= 24 AND prior_status_asof='OPEN')   AS open_24h,

    COUNTIF(age_h <= 72)                                AS buys_72h,
    COUNTIF(age_h <= 72 AND prior_status_asof='TP20')   AS wins_72h,
    COUNTIF(age_h <= 72 AND prior_status_asof='SL25')   AS losses_72h,
    COUNTIF(age_h <= 72 AND prior_status_asof='OPEN')   AS open_72h,

    COUNT(*)                                            AS buys_7d,
    COUNTIF(prior_status_asof='TP20')                   AS wins_7d,
    COUNTIF(prior_status_asof='SL25')                   AS losses_7d,
    COUNTIF(prior_status_asof='OPEN')                   AS open_7d,

    MIN(IF(prior_status_asof='TP20', age_h, NULL))      AS hrs_since_last_win,
    MIN(IF(prior_status_asof='SL25', age_h, NULL))      AS hrs_since_last_loss
  FROM status_asof s
  GROUP BY 1,2,3
),

-- 7) Decay-weighted outcomes (half-life 24h) over 7d
decay AS (
  SELECT
    s.current_trade_id,
    SUM(CASE WHEN prior_status_asof='TP20' THEN POW(0.5, age_h/24.0) ELSE 0 END) AS decayed_wins_7d,
    SUM(CASE WHEN prior_status_asof='SL25' THEN POW(0.5, age_h/24.0) ELSE 0 END) AS decayed_losses_7d
  FROM status_asof s
  GROUP BY 1
),

-- 8) All current candidates (tokens with no priors still appear with zeros)
all_current AS (
  SELECT
    e.trade_id     AS current_trade_id,
    e.token_address,
    e.decision_ts  AS current_dt,
    e.entry_price  AS current_entry_price,
    e.prob_buy     AS current_prob_buy
  FROM entries e
)

-- Final
SELECT
  a.current_trade_id,
  a.token_address,
  a.current_dt,
  a.current_entry_price,
  a.current_prob_buy,

  COALESCE(g.buys_12h,0)   AS buys_12h,
  COALESCE(g.wins_12h,0)   AS wins_12h,
  COALESCE(g.losses_12h,0) AS losses_12h,
  COALESCE(g.open_12h,0)   AS open_12h,

  COALESCE(g.buys_24h,0)   AS buys_24h,
  COALESCE(g.wins_24h,0)   AS wins_24h,
  COALESCE(g.losses_24h,0) AS losses_24h,
  COALESCE(g.open_24h,0)   AS open_24h,

  COALESCE(g.buys_72h,0)   AS buys_72h,
  COALESCE(g.wins_72h,0)   AS wins_72h,
  COALESCE(g.losses_72h,0) AS losses_72h,
  COALESCE(g.open_72h,0)   AS open_72h,

  COALESCE(g.buys_7d,0)    AS buys_7d,
  COALESCE(g.wins_7d,0)    AS wins_7d,
  COALESCE(g.losses_7d,0)  AS losses_7d,
  COALESCE(g.open_7d,0)    AS open_7d,

  SAFE_DIVIDE(g.wins_12h,   NULLIF(g.buys_12h,0)) AS win_rate_12h,
  SAFE_DIVIDE(g.wins_24h,   NULLIF(g.buys_24h,0)) AS win_rate_24h,
  SAFE_DIVIDE(g.wins_72h,   NULLIF(g.buys_72h,0)) AS win_rate_72h,
  SAFE_DIVIDE(g.wins_7d,    NULLIF(g.buys_7d,0))  AS win_rate_7d,

  SAFE_DIVIDE(g.losses_12h, NULLIF(g.buys_12h,0)) AS loss_rate_12h,
  SAFE_DIVIDE(g.losses_24h, NULLIF(g.buys_24h,0)) AS loss_rate_24h,
  SAFE_DIVIDE(g.losses_72h, NULLIF(g.buys_72h,0)) AS loss_rate_72h,
  SAFE_DIVIDE(g.losses_7d,  NULLIF(g.buys_7d,0))  AS loss_rate_7d,

  COALESCE(g.hrs_since_last_win,  999999) AS hrs_since_last_win,
  COALESCE(g.hrs_since_last_loss, 999999) AS hrs_since_last_loss,

  COALESCE(d.decayed_wins_7d,   0.0) AS decayed_wins_7d,
  COALESCE(d.decayed_losses_7d, 0.0) AS decayed_losses_7d
FROM all_current a
LEFT JOIN agg g
  ON g.current_trade_id = a.current_trade_id
LEFT JOIN decay d
  ON d.current_trade_id = a.current_trade_id
