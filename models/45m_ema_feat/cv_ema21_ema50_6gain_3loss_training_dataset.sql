{% set gain_threshold = 0.06 %}  -- 6% as a fraction

SELECT 
    tp.*,
    tm.label_gain6_before_loss3

FROM {{ ref('cv_ema21_ema50_feat') }} tp
INNER JOIN {{ ref('cv_ema21_ema50_6gain_3loss_labels') }} tm
  ON tp.token_address = tm.token_chain_id
 AND tp.event_ts   = tm.entry_ts
WHERE tm.token_address NOT IN (
    SELECT DISTINCT token_address
    FROM {{ ref('scam_h_union') }} s
    WHERE s.chain = tm.chain
)
