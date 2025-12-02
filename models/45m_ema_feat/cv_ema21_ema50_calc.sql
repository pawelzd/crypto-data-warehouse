-- dbt model (BigQuery)
{{ config(materialized='table') }}


{% set len21 = 21 %}
{% set len50 = 50 %}

with base as (
  select
    chain, token_chain_id, token_address,
    price_timestamp,
    cast(price_usd as float64) as price_usd
  from {{ ref('cv_prep_ema21_ema50') }}
),

ordered as (
  select
    b.*,
    row_number() over (partition by token_chain_id order by price_timestamp) as rn
  from base b
),

params as (
  select
    *,
    2.0 / ({{ len21 }} + 1) as alpha21,
    2.0 / ({{ len50 }} + 1) as alpha50
  from ordered
),

-- compute rolling SMA(N) at the Nth bar (seed)
seed as (
  select
    p.*,
    case when rn >= {{ len21 }}
      then avg(price_usd) over (
             partition by token_chain_id
             order by price_timestamp
             rows between {{ len21 - 1 }} preceding and current row
           )
    end as sma21,
    case when rn >= {{ len50 }}
      then avg(price_usd) over (
             partition by token_chain_id
             order by price_timestamp
             rows between {{ len50 - 1 }} preceding and current row
           )
    end as sma50
  from params p
),

ema as (
  select
    s.*,

    -- EMA_21 with SMA seed at bar 21
    case
      when rn < {{ len21 }} then null
      when rn = {{ len21 }} then sma21
      else
        pow(1 - alpha21, rn - {{ len21 }}) * sma21
        + (
          select sum(alpha21 * pow(1 - alpha21, s.rn - w.rn) * w.price_usd)
          from seed w
          where w.token_chain_id = s.token_chain_id
            and w.rn >  {{ len21 }}
            and w.rn <= s.rn
        )
    end as ema_21,

    -- EMA_50 with SMA seed at bar 50
    case
      when rn < {{ len50 }} then null
      when rn = {{ len50 }} then sma50
      else
        pow(1 - alpha50, rn - {{ len50 }}) * sma50
        + (
          select sum(alpha50 * pow(1 - alpha50, s.rn - w.rn) * w.price_usd)
          from seed w
          where w.token_chain_id = s.token_chain_id
            and w.rn >  {{ len50 }}
            and w.rn <= s.rn
        )
    end as ema_50

  from seed s
)

select *
from ema
order by token_chain_id, price_timestamp