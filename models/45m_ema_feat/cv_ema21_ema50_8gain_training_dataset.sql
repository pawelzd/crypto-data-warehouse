{% set gain_threshold = 0.08 %}  -- 6% as a fraction

SELECT 
    tp.*,
    case
    when tm.max_runup is not null and tm.max_runup >= {{ gain_threshold }} then 1
    else 0
  end as label_gain_8pc

FROM {{ ref('cv_ema21_ema50_feat') }} tp
INNER JOIN {{ ref('cv_ema21_ema50_max_gain') }} tm
  ON tp.token_address = tm.token_chain_id
 AND tp.event_ts   = tm.entry_ts
