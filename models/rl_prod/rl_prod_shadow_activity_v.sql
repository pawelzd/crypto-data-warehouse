{{ config(materialized='view') }}

-- Per (cycle, model) intent vs action, cleanly separated so phantom re-affirmations
-- are never confused with real trades:
--   new_entries   = genuine flat->long on a non-warmup bar (a fill if admitted)
--   reaffirm_holds= PHANTOM "stay long" on a position held only in model state
--                   (warm-up entries the ledger never adopted) — no-op, buy_executed=false
--   gate_blocked  = gate047 turned a BUY into HOLD
--   warmup_replay = one-time warm-up replay decisions (never executed to the ledger)
--   admitted_*    = ACTUAL shared-ledger fills
SELECT
  inserted_at                                              AS cycle_ts,
  DATE(inserted_at)                                        AS dt,
  model_id,
  COUNT(*)                                                 AS decisions,
  COUNTIF(raw_action = 1 AND buy_executed
          AND drop_reason != 'warmup')                     AS new_entries,
  COUNTIF(raw_action = 1 AND NOT buy_executed AND action = 1) AS reaffirm_holds,
  COUNTIF(raw_action = 1 AND action != 1)                  AS gate_blocked,
  COUNTIF(drop_reason = 'warmup')                          AS warmup_replay,
  COUNTIF(admitted AND fill_side = 'BUY')                  AS admitted_buys,
  COUNTIF(admitted AND fill_side = 'SELL')                 AS admitted_sells,
  COUNTIF(is_edge)                                         AS edge_decisions
FROM {{ source('rl_prod_artifacts', 'shadow_decisions') }}
GROUP BY inserted_at, model_id
