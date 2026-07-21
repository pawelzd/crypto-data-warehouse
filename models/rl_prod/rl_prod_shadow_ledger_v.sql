{{ config(materialized='view') }}

-- Shadow portfolio curve, one row per hourly cycle (inserted_at). equity_usd is
-- the authoritative mark-to-market computed once per cycle; realized_pnl is
-- cumulative. Power BI: plot equity_usd / realized_pnl over cycle_ts.
SELECT
  inserted_at                                   AS cycle_ts,
  DATE(inserted_at)                             AS dt,
  MAX(timestamp)                                AS latest_decided_bar,
  ANY_VALUE(cycle_equity_usd)                   AS equity_usd,
  MAX(ledger_realized_pnl_usd)                  AS realized_pnl_usd,
  MAX(ledger_n_open)                            AS open_positions,
  MAX(ledger_cash_usd)                          AS cash_usd,
  MAX(ledger_gross_cost_usd)                    AS gross_cost_usd,
  ANY_VALUE(n_cap)                              AS n_cap,
  COUNTIF(admitted AND fill_side = 'BUY')       AS buys_this_cycle,
  COUNTIF(admitted AND fill_side = 'SELL')      AS sells_this_cycle,
  COUNT(*)                                      AS decisions
FROM {{ source('rl_prod_artifacts', 'shadow_decisions') }}
GROUP BY inserted_at
