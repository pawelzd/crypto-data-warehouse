{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='record_id'
) }}

WITH scored AS (
  SELECT
    -- deterministic trade id (stable across reruns)
    *
  FROM ML.PREDICT(
    --MODEL `crypto-trading-474111.ml.forest_72_gain20_loss25_relonly_20m_soft_cutoff_weight`,
    MODEL `crypto-trading-474111.ml.forest_72_gain20_loss25_relonly_20m`,
    (
      SELECT
        *
      FROM {{ref('20m_cv_prod_eval_dataset')}}
      WHERE decision_ts >= TIMESTAMP('2025-05-01')
      {% if is_incremental() %}
        AND decision_ts >
          TIMESTAMP_SUB(
            (SELECT COALESCE(MAX(decision_ts), TIMESTAMP('1970-01-01')) FROM {{ this }}),
            INTERVAL 6 HOUR
          )
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
    FROM UNNEST(predicted_label_profit20_before_loss25_probs) AS p
    WHERE p.label = 1
  ) >= 0.789
),

fwd AS (
  SELECT
    TO_HEX(MD5(CONCAT(
      CAST(e.token_address AS STRING), '|',
      CAST(e.decision_ts   AS STRING), '|',
      CAST(h.price_timestamp      AS STRING)
    ))) AS record_id,
    TO_HEX(MD5(CONCAT(
      CAST(e.token_address AS STRING), '|',
      CAST(e.decision_ts   AS STRING)
    ))) AS trade_id,
    e.token_address,
    e.decision_ts,
    e.price,
    h.price_timestamp AS hour_ts,
    h.close    AS value_now,
    SAFE_DIVIDE(h.close, e.price) - 1 AS ret,
(
  SELECT p.prob
  FROM UNNEST(predicted_label_profit20_before_loss25_probs) AS p
  WHERE p.label = 1
) AS predicted_prob
  FROM base_entries e
  JOIN {{ ref('token_ohlcv') }} h
    ON h.token_address  = e.token_address
   AND h.price_timestamp >= e.decision_ts
   AND TIMESTAMP_DIFF(h.price_timestamp, e.decision_ts, HOUR) BETWEEN 0 AND 72
)

SELECT * FROM fwd
