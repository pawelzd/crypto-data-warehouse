-- Predict next-24h trend labels on 1h prices
-- Output: one row per acquisition timestamp with forward stats + 3-class label

{{ config(
    schema='feat',
    materialized='table'
) }}

{# -----------------------------
   Parameters (override via dbt vars)
   ----------------------------- #}
{% set H_ENTRY = var('h_entry', 8) %}
{% set H_MANAGE = var('h_manage', 2) %}
{% set H_MAX = [H_ENTRY, H_MANAGE] | max %}

{# thresholds for "entry" horizon #}
{% set THR_RET_ENTRY = var('thr_ret_entry', 0.010) %}   {# ~+1.0% log #}
{% set THR_R2_ENTRY  = var('thr_r2_entry', 0.50)  %}
{% set THR_MAE_ENTRY = var('thr_mae_entry', -0.006) %}

{# thresholds for "manage" horizon #}
{% set THR_RET_MANAGE = var('thr_ret_manage', 0.003) %} {# ~+0.3% log #}
{% set THR_R2_MANAGE  = var('thr_r2_manage', 0.40)  %}
{% set THR_MAE_MANAGE = var('thr_mae_manage', -0.004) %}

{# counts of points in inclusive windows [0..H] #}
{% set N_ENTRY = H_ENTRY + 1 %}
{% set N_MANAGE = H_MANAGE + 1 %}

with
/* 1) Hourly prices from extended windows (keep session metadata) */
hourly as (
  select
    token_address,
    monitoring_session_id,
    session_start,
    session_end,
    extended_start,
    extended_end,
    in_pre_extension,
    in_core_monitoring,
    in_post_extension,
    timestamp_trunc(price_timestamp, hour) as ts_hour,
    avg(cast(price_usd as float64)) as price
  from `crypto-trading-474111`.`feat`.`cv_filter_prep_72_ext_windows`
  where price_usd is not null
  group by
    token_address, monitoring_session_id, session_start, session_end,
    extended_start, extended_end, in_pre_extension, in_core_monitoring, in_post_extension, ts_hour
),

/* 2) Candidate acquisition timestamps (ensure visibility up to max horizon) */
acquisitions as (
  select
    token_address,
    monitoring_session_id,
    session_start,
    session_end,
    extended_start,
    extended_end,
    ts_hour as first_acquired_timestamp
  from hourly
  where in_core_monitoring = true
    and timestamp_add(ts_hour, interval {{ H_MAX }} hour) <= extended_end
),

/* 3) Build the forward window [h=0 .. h=H_MAX] per acquisition */
base as (
  select
    a.token_address,
    a.monitoring_session_id,
    a.session_start,
    a.session_end,
    a.extended_start,
    a.extended_end,
    a.first_acquired_timestamp,
    h.ts_hour,
    cast(h.price as float64) as price,
    cast(timestamp_diff(h.ts_hour, a.first_acquired_timestamp, hour) as int64) as h
  from acquisitions a
  join hourly h
    on h.token_address         = a.token_address
   and h.monitoring_session_id = a.monitoring_session_id
   and h.ts_hour between a.first_acquired_timestamp
                    and timestamp_add(a.first_acquired_timestamp, interval {{ H_MAX }} hour)
),

/* 4) Per-hour sequence + helpers */
seq as (
  select
    b.*,
    -- entry price (h=0)
    max(if(b.h = 0, b.price, null)) over (
      partition by b.token_address, b.monitoring_session_id, b.first_acquired_timestamp
    ) as price0,

    -- simple per-hour log return (guarded)
    case
      when b.price > 0 and lag(b.price) over (
        partition by b.token_address, b.monitoring_session_id, b.first_acquired_timestamp
        order by b.h
      ) > 0
      then log(b.price) - log(lag(b.price) over (
        partition by b.token_address, b.monitoring_session_id, b.first_acquired_timestamp
        order by b.h
      ))
      else null
    end as logret_1h,

    -- cum return from entry
    safe_divide(b.price,
      max(if(b.h = 0, b.price, null)) over (
        partition by b.token_address, b.monitoring_session_id, b.first_acquired_timestamp
      )
    ) - 1 as cumret_from_entry,

    -- index for OLS helpers
    row_number() over (
      partition by b.token_address, b.monitoring_session_id, b.first_acquired_timestamp
      order by b.h
    ) as idx,

    -- log price (guarded)
    log(nullif(b.price, 0)) as logp
  from base b
),

/* 5) Forward metrics for both horizons (computed at each h, later picked at h=0) */
fwd as (
  select
    s.*,

    -- arrays for entry & manage horizons
    array_agg(price) over wf_entry  as arr_price_entry,
    array_agg(price) over wf_manage as arr_price_manage,

    -- forward prices at +H
    lead(price, {{ H_ENTRY }})  over w as price_fwd_entry,
    lead(price, {{ H_MANAGE }}) over w as price_fwd_manage,

    /* OLS slope & R^2 (log price vs idx) for ENTRY window */
    (
      (sum(idx*logp) over wf_entry - (sum(idx) over wf_entry)*(sum(logp) over wf_entry)/{{ N_ENTRY }} )
      / nullif( (sum(idx*idx) over wf_entry - (sum(idx) over wf_entry)*(sum(idx) over wf_entry)/{{ N_ENTRY }} ), 0 )
    ) as slope_entry,
    pow(
      (sum(idx*logp) over wf_entry - (sum(idx) over wf_entry)*(sum(logp) over wf_entry)/{{ N_ENTRY }}),
      2
    )
    / nullif(
      (sum(idx*idx) over wf_entry - (sum(idx) over wf_entry)*(sum(idx) over wf_entry)/{{ N_ENTRY }})
      * (sum(logp*logp) over wf_entry - (sum(logp) over wf_entry)*(sum(logp) over wf_entry)/{{ N_ENTRY }}),
      0
    ) as r2_entry,

    /* OLS slope & R^2 for MANAGE window */
    (
      (sum(idx*logp) over wf_manage - (sum(idx) over wf_manage)*(sum(logp) over wf_manage)/{{ N_MANAGE }} )
      / nullif( (sum(idx*idx) over wf_manage - (sum(idx) over wf_manage)*(sum(idx) over wf_manage)/{{ N_MANAGE }} ), 0 )
    ) as slope_manage,
    pow(
      (sum(idx*logp) over wf_manage - (sum(idx) over wf_manage)*(sum(logp) over wf_manage)/{{ N_MANAGE }}),
      2
    )
    / nullif(
      (sum(idx*idx) over wf_manage - (sum(idx) over wf_manage)*(sum(idx) over wf_manage)/{{ N_MANAGE }})
      * (sum(logp*logp) over wf_manage - (sum(logp) over wf_manage)*(sum(logp) over wf_manage)/{{ N_MANAGE }}),
      0
    ) as r2_manage

  from seq s
  window
    w          as (partition by token_address, monitoring_session_id, first_acquired_timestamp order by h),
    wf_entry   as (partition by token_address, monitoring_session_id, first_acquired_timestamp
                   order by h rows between current row and {{ H_ENTRY }} following),
    wf_manage  as (partition by token_address, monitoring_session_id, first_acquired_timestamp
                   order by h rows between current row and {{ H_MANAGE }} following)
),

/* 6) Collapse to one row per acquisition; pick values at h=0 */
per_acq as (
  select
    token_address,
    monitoring_session_id,
    first_acquired_timestamp,
    session_start,
    session_end,
    extended_start,
    extended_end,

    -- completeness per horizon
    count(*) as n_points_0_maxh,
    max(h)   as max_h_0_maxh,

    -- entry horizon completeness (0..H_ENTRY)
    max(case when h = 0 then 0 else null end) as _dummy,  -- keeps group
    min(h) as min_h,
    max(h) as max_h,
    case when count(*) >= {{ N_ENTRY }} and min(h) = 0 and max(h) >= {{ H_ENTRY }} then 1 else 0 end as has_full_entry,
    case when count(*) >= {{ N_MANAGE }} and min(h) = 0 and max(h) >= {{ H_MANAGE }} then 1 else 0 end as has_full_manage,

    -- entry price
    max(case when h = 0 then price0 end) as price0,

    -- forward end states (log returns) at +H, evaluated at h=0
    max(case when h = 0 and price0 > 0 and price_fwd_entry  > 0 then log(price_fwd_entry  / price0) end) as fwd_logret_entry,
    max(case when h = 0 and price0 > 0 and price_fwd_manage > 0 then log(price_fwd_manage / price0) end) as fwd_logret_manage,

    -- path cleanliness at h=0
    max(case when h = 0 then r2_entry  end) as r2_entry,
    max(case when h = 0 then r2_manage end) as r2_manage,

    -- MFE/MAE over horizon windows, computed at h=0
    max(case when h = 0 and price0 > 0
             then log( (select max(p) from unnest(arr_price_entry)  p) / price0 ) end) as mfe_entry,
    max(case when h = 0 and price0 > 0
             then log( (select min(p) from unnest(arr_price_entry)  p) / price0 ) end) as mae_entry,

    max(case when h = 0 and price0 > 0
             then log( (select max(p) from unnest(arr_price_manage) p) / price0 ) end) as mfe_manage,
    max(case when h = 0 and price0 > 0
             then log( (select min(p) from unnest(arr_price_manage) p) / price0 ) end) as mae_manage

  from fwd
  group by
    token_address, monitoring_session_id, first_acquired_timestamp,
    session_start, session_end, extended_start, extended_end
),

/* 7) Apply tunable labeling rules for both horizons */
rules as (
  select
    *,
    {{ THR_RET_ENTRY  }} as thr_ret_entry,
    {{ THR_R2_ENTRY   }} as thr_r2_entry,
    {{ THR_MAE_ENTRY  }} as thr_mae_entry,
    {{ THR_RET_MANAGE }} as thr_ret_manage,
    {{ THR_R2_MANAGE  }} as thr_r2_manage,
    {{ THR_MAE_MANAGE }} as thr_mae_manage
  from per_acq
)

-- 8) Final output: one row per acquisition with both labels
select
  token_address,
  monitoring_session_id,
  first_acquired_timestamp,

  -- completeness
  has_full_entry,
  has_full_manage,

  -- entry-horizon metrics (H = {{ H_ENTRY }}h)
  price0,
  fwd_logret_entry,
  r2_entry,
  mfe_entry,
  mae_entry,
  case
    when fwd_logret_entry >=  thr_ret_entry and r2_entry >= thr_r2_entry and mae_entry >= thr_mae_entry then  1
    when fwd_logret_entry <= -thr_ret_entry and r2_entry >= thr_r2_entry and mfe_entry <= -thr_mae_entry then -1
    else 0
  end as label_entry_k{{ H_ENTRY }},

  -- manage-horizon metrics (H = {{ H_MANAGE }}h)
  fwd_logret_manage,
  r2_manage,
  mfe_manage,
  mae_manage,
  case
    when fwd_logret_manage >=  thr_ret_manage and r2_manage >= thr_r2_manage and mae_manage >= thr_mae_manage then  1
    when fwd_logret_manage <= -thr_ret_manage and r2_manage >= thr_r2_manage and mfe_manage <= -thr_mae_manage then -1
    else 0
  end as label_manage_k{{ H_MANAGE }}

from rules
