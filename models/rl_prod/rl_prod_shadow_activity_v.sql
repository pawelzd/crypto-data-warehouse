{{ config(materialized='view') }}

-- Per (cycle, model) intent vs action, counted as DISTINCT TOKENS so the numbers
-- are stable and comparable across cycles. (Raw decision-row counts swing because
-- a cycle decides a VARIABLE number of bars per token; token counts don't.)
--   new_entries       = tokens with a genuine flat->long on a non-warmup bar
--   reaffirm_holds    = tokens re-affirmed "stay long" while held ONLY in model
--                       state (phantom warm-up holds the ledger never adopted)
--   gate_blocked      = tokens whose BUY gate047 turned into HOLD
--   warmup_replay     = tokens touched by the one-time warm-up replay
--   admitted_*        = ACTUAL shared-ledger fills (one event per token)
--   avg_bars_per_token= bars decided per token: ~1 nothing new settled, ~2 normal
--                       (newly-settled bar + provisional edge bar), >2 backlog catch-up
--   decision_rows     = raw row count (tokens x bars), for reference
SELECT
  inserted_at                                   AS cycle_ts,
  DATE(inserted_at)                             AS dt,
  model_id,
  COUNT(DISTINCT token)                         AS tokens,
  ROUND(COUNT(*) / COUNT(DISTINCT token), 2)    AS avg_bars_per_token,
  COUNT(DISTINCT IF(raw_action = 1 AND buy_executed
        AND drop_reason != 'warmup', token, NULL))            AS new_entries,
  COUNT(DISTINCT IF(raw_action = 1 AND NOT buy_executed
        AND action = 1, token, NULL))                         AS reaffirm_holds,
  COUNT(DISTINCT IF(raw_action = 1 AND action != 1, token, NULL)) AS gate_blocked,
  COUNT(DISTINCT IF(drop_reason = 'warmup', token, NULL))     AS warmup_replay,
  COUNTIF(admitted AND fill_side = 'BUY')       AS admitted_buys,
  COUNTIF(admitted AND fill_side = 'SELL')      AS admitted_sells,
  COUNT(*)                                      AS decision_rows
FROM {{ source('rl_prod_artifacts', 'shadow_decisions') }}
GROUP BY inserted_at, model_id
