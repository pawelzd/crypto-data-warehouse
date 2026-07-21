{{ config(materialized='view') }}

-- Trade blotter: one row per ACTUAL admitted fill (BUY/SELL). Empty while the
-- shadow is flat; the place to watch when it starts trading. fill_slippage_bps
-- is the all-in realized cost vs the reference price (fee + impact folded in).
SELECT
  inserted_at            AS cycle_ts,
  timestamp              AS bar_ts,
  model_id,
  token,
  fill_side,
  fill_price,
  fill_size_usd,
  fill_fee_bps,
  fill_slippage_bps,
  fill_simulated,
  p_t                    AS ref_price,
  step_log_net,
  is_edge
FROM {{ source('rl_prod_artifacts', 'shadow_decisions') }}
WHERE admitted AND fill_side IN ('BUY', 'SELL')
