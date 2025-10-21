WITH emas AS (
  SELECT
    e.*
  FROM {{ ref('cv_ema21_ema50_pct_gain') }} e
)
  SELECT
    t.*,
    e.ema_21,
    e.ema_50,
    CASE 
        WHEN e.label_entry = 1 
         AND e.entry_price IS NOT NULL 
         AND e.max_price_until_close IS NOT NULL
         AND e.max_price_until_close >= e.entry_price * 1.06
        THEN 1 ELSE 0 
    END AS ema21_ema50_6gain
  FROM emas e
  INNER JOIN {{ ref('20m_cv_filter_72_training_dataset') }} t
    ON t.token_address = e.token_address
   AND t.decision_ts       = e.ts_hour
  WHERE e.label_entry = 1