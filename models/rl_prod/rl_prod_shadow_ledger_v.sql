{{ config(materialized='view') }}

-- Shadow portfolio curve, one row per hourly cycle. equity_usd is the
-- authoritative mark-to-market computed once per cycle; realized_pnl is cumulative.
--   open_positions    = REAL positions in the shared ledger (what actually traded)
--   model_state_holds = (model, token) states with pos>0, INCLUDING phantom warm-up
--                       holds the ledger never adopted. When it exceeds
--                       open_positions there is a phantom overhang (models "invested
--                       in their head" but no real position).
SELECT
  inserted_at                                   AS cycle_ts,
  DATE(inserted_at)                             AS dt,
  MAX(timestamp)                                AS latest_decided_bar,
  ANY_VALUE(cycle_equity_usd)                   AS equity_usd,
  MAX(ledger_realized_pnl_usd)                  AS realized_pnl_usd,
  MAX(ledger_n_open)                            AS open_positions,
  COUNT(DISTINCT IF(pos > 0, FORMAT('%s|%s', model_id, token), NULL)) AS model_state_holds,
  MAX(ledger_cash_usd)                          AS cash_usd,
  ANY_VALUE(n_cap)                              AS n_cap,
  COUNTIF(admitted AND fill_side = 'BUY')       AS admitted_buys,
  COUNTIF(admitted AND fill_side = 'SELL')      AS admitted_sells,
  COUNT(*)                                      AS decisions
FROM {{ source('rl_prod_artifacts', 'shadow_decisions') }}
GROUP BY inserted_at
