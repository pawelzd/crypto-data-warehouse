with eligible as (
  select *
  from {{ ref('cv_ema21_ema50_labels') }}
  where hours_since_first >= 50
    and (
      (chain = 'sol'  and mktcap >= 100000000) or
      (chain <> 'sol' and mktcap >=  45000000)
    )
),

-- For every row, find the timestamp of the next label_close on the same token
next_close_mark as (
  select
    e.*,
    (
      select min(price_timestamp)
      from eligible x
      where x.token_chain_id = e.token_chain_id
        and x.price_timestamp  > e.price_timestamp
        and x.label_close = 1
    ) as next_close_ts
  from eligible e
),

-- Keep only the actual entries that have a subsequent close
entries as (
  select *
  from next_close_mark
  where label_entry = 1
    and next_close_ts is not null
),

-- Get the exit price at the close timestamp
exits as (
  select
    ent.*,
    ex.price_usd as exit_price_usd
  from entries ent
  join eligible ex
    on ex.token_chain_id  = ent.token_chain_id
   and ex.price_timestamp = ent.next_close_ts
),

-- Compute window stats between entry and close (max run-up, max drawdown, etc.)
window_stats as (
  select
    ex.*,
    -- best price in [entry_ts, next_close_ts]
    (
      select max(w.price_usd)
      from eligible w
      where w.token_chain_id = ex.token_chain_id
        and w.price_timestamp >= ex.price_timestamp
        and w.price_timestamp <= ex.next_close_ts
    ) as max_price_usd_in_window,
    -- worst price in [entry_ts, next_close_ts] (for max drawdown)
    (
      select min(w.price_usd)
      from eligible w
      where w.token_chain_id = ex.token_chain_id
        and w.price_timestamp >= ex.price_timestamp
        and w.price_timestamp <= ex.next_close_ts
    ) as min_price_usd_in_window,
    -- number of bars inside the trade
    (
      select count(*)
      from eligible w
      where w.token_chain_id = ex.token_chain_id
        and w.price_timestamp >  ex.price_timestamp
        and w.price_timestamp <= ex.next_close_ts
    ) as bars_to_close
  from exits ex
)

-- Final trade metrics per entry signal
select
  token_chain_id,
  token_address,
  chain,
  mktcap,
  price_timestamp          as entry_ts,
  price_usd                as entry_price_usd,
  next_close_ts            as close_ts,
  exit_price_usd,
  max_price_usd_in_window,
  min_price_usd_in_window,
  bars_to_close,

  -- realized return if you must exit at the first subsequent label_close
  safe_divide(exit_price_usd - price_usd, price_usd)                          as realized_return,

  -- "maximal percentage gain" achievable before that close (peak in the window)
  safe_divide(max_price_usd_in_window - price_usd, price_usd)                 as max_runup,

  -- drawdown into the close (optional diagnostic)
  safe_divide(exit_price_usd - max_price_usd_in_window, max_price_usd_in_window) as pullback_from_peak,

  -- MAX DRAWDOWN: most adverse move from entry to the worst price in the window
  safe_divide(min_price_usd_in_window - price_usd, price_usd)                 as max_drawdown
from window_stats
order by entry_ts