{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='trade_id'
) }}

WITH scored AS (
  SELECT
    -- deterministic trade id (stable across reruns)
    TO_HEX(MD5(CONCAT(
      CAST(token_address AS STRING), '|',
      CAST(decision_ts   AS STRING), '|'
    ))) AS trade_id,
    *
  FROM ML.PREDICT(
    MODEL `crypto-trading-474111.ml.forest_72_gain20_loss25_relonly_20m_soft_cutoff_weight`,
    (
      SELECT
        *
      FROM `crypto-trading-474111.20m_eval.20m_cv_prod_eval_dataset`
      WHERE decision_ts >= TIMESTAMP('2025-12-12')
      {% if is_incremental() %}
        AND decision_ts >
          (SELECT COALESCE(MAX(decision_ts), TIMESTAMP('1970-01-01')) FROM {{ this }})
      {% endif %}
    )
  )
),

base_entries AS (
  SELECT
    *
  FROM scored
  WHERE (
    SELECT p.prob
    FROM UNNEST(predicted_label_profit10_before_loss25_probs) AS p
    WHERE p.label = 1
  ) >= 0.759
),

fwd AS (
  SELECT
    e.trade_id,
    e.token_address,
    e.decision_ts,
    e.price,
    h.datetime AS hour_ts,
    h.price    AS value_now,
    SAFE_DIVIDE(h.price, e.price) - 1 AS ret
  FROM base_entries e
  JOIN {{ source('streamed_datapublic', 'public_historical_prices') }} h
    ON h.address  = e.token_address
   AND h.datetime >= e.decision_ts
   AND TIMESTAMP_DIFF(h.datetime, e.decision_ts, HOUR) BETWEEN 0 AND 72
)

SELECT * FROM fwd
