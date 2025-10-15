{% set label_pos = 1 %}
{% set label_neg = 0 %}
{% set ratio_pos = 0.40 %}
{% set ratio_neg = 0.60 %}

WITH base AS (
  SELECT *
  FROM {{ ref('cv_filter_72_training_dataset_trend_restricted') }}
),
base_keys AS (
  SELECT
    token_address,
    decision_ts,
    label_uptrend_8h AS label,
    ABS(FARM_FINGERPRINT(CONCAT(CAST(decision_ts AS STRING),'|', IFNULL(token_address,'')))) AS h
  FROM base
),

-- counts and targets
counts AS (
  SELECT
    SUM(CASE WHEN label = {{ label_pos }} THEN 1 ELSE 0 END) AS pos_cnt,
    SUM(CASE WHEN label = {{ label_neg }} THEN 1 ELSE 0 END) AS neg_cnt
  FROM base_keys
),
params AS (
  SELECT
    pos_cnt, neg_cnt,
    LEAST(pos_cnt / {{ ratio_pos }}, neg_cnt / {{ ratio_neg }}) AS k,
    {{ ratio_pos }} AS ratio_pos, {{ ratio_neg }} AS ratio_neg
  FROM counts
),
targets AS (
  SELECT
    CAST(FLOOR(ratio_pos * k) AS INT64) AS target_pos,
    CAST(FLOOR(ratio_neg * k) AS INT64) AS target_neg
  FROM params
),

-- pick K smallest hashes per class without LIMIT subquery
pos_keys AS (
  SELECT token_address, decision_ts
  FROM (
    SELECT
      token_address,
      decision_ts,
      ROW_NUMBER() OVER (ORDER BY h) AS rn
    FROM base_keys
    WHERE label = {{ label_pos }}
  ) pk
  CROSS JOIN targets t
  WHERE pk.rn <= t.target_pos
),
neg_keys AS (
  SELECT token_address, decision_ts
  FROM (
    SELECT
      token_address,
      decision_ts,
      ROW_NUMBER() OVER (ORDER BY h) AS rn
    FROM base_keys
    WHERE label = {{ label_neg }}
  ) nk
  CROSS JOIN targets t
  WHERE nk.rn <= t.target_neg
),

-- rejoin to wide rows only for selected keys
pos_final AS (
  SELECT b.*
  FROM base b
  JOIN pos_keys k USING (token_address, decision_ts)
),
neg_final AS (
  SELECT b.*
  FROM base b
  JOIN neg_keys k USING (token_address, decision_ts)
)

SELECT * FROM pos_final
UNION ALL
SELECT * FROM neg_final