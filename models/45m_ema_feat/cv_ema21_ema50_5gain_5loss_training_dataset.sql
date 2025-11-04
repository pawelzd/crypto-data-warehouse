{% set gain_threshold = 0.08 %}  -- 6% as a fraction

SELECT 
    tp.*,
    tm.label_gain5_before_loss5

FROM {{ ref('cv_ema21_ema50_feat') }} tp
INNER JOIN {{ ref('cv_ema21_ema50_5gain_5loss_labels') }} tm
  ON tp.token_address = tm.token_chain_id
 AND tp.event_ts   = tm.entry_ts
