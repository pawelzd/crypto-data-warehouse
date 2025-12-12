{{ config(materialized='table') }}

{% set gain_thr = 0.06 %}   -- +5%
{% set loss_thr = 0.03 %}   -- -5%
{% set horizon_hours = 48 %} -- cap lookahead

-- 1) Universe (same filter you use)
with eligible as (
  select *
  from {{ ref('cv_ema21_ema50_labels') }}
  where hours_since_first >= 50
    and (
      (chain = 'sol'  and mktcap >= 100000000) or
      (chain <> 'sol' and mktcap >=  45000000)
    )
),

-- 2) Track most recent close to suppress overlapping entries
tagged as (
  select
    e.*,
    max(if(label_close=1, price_timestamp, null)) over (
      partition by token_chain_id
      order by price_timestamp
      rows between unbounded preceding and current row
    ) as prev_close_ts
  from eligible e
),

-- 3) Keep only the first entry after the previous close
entries_raw as (
  select *
  from tagged
  where label_entry = 1
  qualify row_number() over (
    partition by token_chain_id, prev_close_ts
    order by price_timestamp
  ) = 1
),

-- 4) Find the next close timestamp (exit boundary candidate #1)
next_close_mark as (
  select
    e.*,
    (
      select min(x.price_timestamp)
      from eligible x
      where x.token_chain_id = e.token_chain_id
        and x.price_timestamp  > e.price_timestamp
        and x.label_close = 1
    ) as next_close_ts
  from entries_raw e
),

-- 5) Compute the final boundary: min(next_close, entry + horizon)
entry_bounds as (
  select
    n.*,
    timestamp_add(n.price_timestamp, interval {{ horizon_hours }} hour) as entry_plus_horizon_ts,
    case
      when n.next_close_ts is null then timestamp_add(n.price_timestamp, interval {{ horizon_hours }} hour)
      when n.next_close_ts <= timestamp_add(n.price_timestamp, interval {{ horizon_hours }} hour) then n.next_close_ts
      else timestamp_add(n.price_timestamp, interval {{ horizon_hours }} hour)
    end as boundary_ts
  from next_close_mark n
),

-- 6) First time we hit +8% or -5% within [entry, boundary]
first_hits as (
  select
    b.*,
    -- threshold prices
    b.price_usd * (1 + {{ gain_thr }}) as tgt_gain_px,
    b.price_usd * (1 - {{ loss_thr }}) as tgt_loss_px,

    -- earliest timestamp where price >= +8%
    (
      select min(w.price_timestamp)
      from eligible w
      where w.token_chain_id = b.token_chain_id
        and w.price_timestamp >= b.price_timestamp
        and w.price_timestamp <= b.boundary_ts
        and w.price_usd >= b.price_usd * (1 + {{ gain_thr }})
    ) as gain_hit_ts,

    -- earliest timestamp where price <= -5%
    (
      select min(w.price_timestamp)
      from eligible w
      where w.token_chain_id = b.token_chain_id
        and w.price_timestamp >= b.price_timestamp
        and w.price_timestamp <= b.boundary_ts
        and w.price_usd <= b.price_usd * (1 - {{ loss_thr }})
    ) as loss_hit_ts
  from entry_bounds b
)

-- 7) Final label: +8% before -5% within the window
select
  token_chain_id,
  token_address,
  chain,
  price_timestamp as entry_ts,
  price_usd       as entry_px,
  next_close_ts,
  entry_plus_horizon_ts,
  boundary_ts,
  gain_hit_ts,
  loss_hit_ts,

  case
    when gain_hit_ts is not null
         and (loss_hit_ts is null or gain_hit_ts <= loss_hit_ts)
      then 1 else 0
  end as label_gain6_before_loss3,

  -- optional diagnostics
  case when gain_hit_ts is not null then timestamp_diff(gain_hit_ts, price_timestamp, minute) end as minutes_to_gain,
  case when loss_hit_ts is not null then timestamp_diff(loss_hit_ts, price_timestamp, minute) end as minutes_to_loss
from first_hits
order by price_timestamp
