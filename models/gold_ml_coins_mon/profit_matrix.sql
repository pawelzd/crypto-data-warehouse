{{ config(materialized='view') }}

-- Standalone matrix of counts: up threshold hit before down threshold
WITH src AS (
  SELECT *
  FROM {{ ref('price_filter_features_7daysafter') }}  -- <-- replace with your model that has t_hit_* columns
),

pairwise AS (
  SELECT
    up.name AS up_label,
    dn.name AS dn_label,
    CASE WHEN up.t < dn.t THEN 1 ELSE 0 END AS up_before_dn
  FROM src s,
  UNNEST([
    STRUCT('up_10'  AS name, t_hit_up_10  AS t),
    STRUCT('up_15'  AS name, t_hit_up_15  AS t),
    STRUCT('up_20'  AS name, t_hit_up_20  AS t),
    STRUCT('up_25'  AS name, t_hit_up_25  AS t),
    STRUCT('up_30'  AS name, t_hit_up_30  AS t),
    STRUCT('up_35'  AS name, t_hit_up_35  AS t),
    STRUCT('up_40'  AS name, t_hit_up_40  AS t),
    STRUCT('up_45'  AS name, t_hit_up_45  AS t),
    STRUCT('up_50'  AS name, t_hit_up_50  AS t),
    STRUCT('up_100' AS name, t_hit_up_100 AS t),
    STRUCT('up_200' AS name, t_hit_up_200 AS t),
    STRUCT('up_500' AS name, t_hit_up_500 AS t)
  ]) AS up,
  UNNEST([
    STRUCT('dn_10' AS name, t_hit_dn_10 AS t),
    STRUCT('dn_15' AS name, t_hit_dn_15 AS t),
    STRUCT('dn_20' AS name, t_hit_dn_20 AS t),
    STRUCT('dn_25' AS name, t_hit_dn_25 AS t),
    STRUCT('dn_30' AS name, t_hit_dn_30 AS t),
    STRUCT('dn_35' AS name, t_hit_dn_35 AS t),
    STRUCT('dn_40' AS name, t_hit_dn_40 AS t),
    STRUCT('dn_45' AS name, t_hit_dn_45 AS t),
    STRUCT('dn_50' AS name, t_hit_dn_50 AS t)
  ]) AS dn
),

pair_counts AS (
  SELECT
    up_label,
    dn_label,
    SUM(up_before_dn) AS cnt_up_before_dn
  FROM pairwise
  GROUP BY up_label, dn_label
)

SELECT
  up_label,
  SUM(IF(dn_label = 'dn_10', cnt_up_before_dn, 0)) AS dn_10,
  SUM(IF(dn_label = 'dn_15', cnt_up_before_dn, 0)) AS dn_15,
  SUM(IF(dn_label = 'dn_20', cnt_up_before_dn, 0)) AS dn_20,
  SUM(IF(dn_label = 'dn_25', cnt_up_before_dn, 0)) AS dn_25,
  SUM(IF(dn_label = 'dn_30', cnt_up_before_dn, 0)) AS dn_30,
  SUM(IF(dn_label = 'dn_35', cnt_up_before_dn, 0)) AS dn_35,
  SUM(IF(dn_label = 'dn_40', cnt_up_before_dn, 0)) AS dn_40,
  SUM(IF(dn_label = 'dn_45', cnt_up_before_dn, 0)) AS dn_45,
  SUM(IF(dn_label = 'dn_50', cnt_up_before_dn, 0)) AS dn_50
FROM pair_counts
GROUP BY up_label
ORDER BY up_label
