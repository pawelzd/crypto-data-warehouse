{{ config(materialized='view') }}

-- Per (cycle, model) decision breakdown — the "why flat / what did each model do"
-- dashboard. buy_signals = raw BUY output; the drop_reason columns explain why a
-- signal was not admitted; edge_bars are provisional freshest-bar decisions.
SELECT
  inserted_at                                   AS cycle_ts,
  DATE(inserted_at)                             AS dt,
  model_id,
  COUNT(*)                                      AS decisions,
  COUNTIF(raw_action = 1)                       AS buy_signals,
  COUNTIF(gate_blocked)                         AS gate_blocked,
  COUNTIF(drop_reason = 'warmup')               AS warmup_dropped,
  COUNTIF(drop_reason = 'slot_cap')             AS slot_capped,
  COUNTIF(drop_reason = 'token_held')           AS token_held,
  COUNTIF(admitted AND fill_side = 'BUY')       AS buys_admitted,
  COUNTIF(admitted AND fill_side = 'SELL')      AS sells,
  COUNTIF(is_edge)                              AS edge_bars
FROM {{ source('rl_prod_artifacts', 'shadow_decisions') }}
GROUP BY inserted_at, model_id
