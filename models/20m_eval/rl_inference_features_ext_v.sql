{{ config(
    materialized = 'view'
) }}

-- ============================================================================
-- rl_inference_features_next_open_v + the offline side-pipeline blocks that
-- cannot be built in SQL: A2 lead-lag network and Tier C (spec 2026-07-08 §4/§6).
--
-- The A2/C tables are produced daily by the external Python job
-- (features-extra/daily_info_structure_features.py) and registered as the
-- `features_extra` dbt source. This is a SEPARATE downstream view rather than
-- folded into the base model on purpose: the daily job READS the base model to
-- PRODUCE those tables, so joining them back into the base would create a cycle.
-- LEFT JOINs mean a missing daily run degrades to 0 / NULL defaults, not an error.
--
-- No-lookahead (A2): a leadlag row with run_date = D was computed after calendar
-- day D closed, so each hourly row uses the latest run_date STRICTLY BEFORE
-- DATE(price_timestamp).
-- ============================================================================
WITH base AS (
  SELECT * FROM {{ ref('rl_inference_features_next_open_v') }}
),

-- latest applicable leader set / score per (token, hour)
a2_effective AS (
  SELECT
    b.token_address,
    b.price_timestamp,
    s.run_date,
    COALESCE(s.lead_score_90d, 0.0) AS lead_score_90d
  FROM base b
  LEFT JOIN {{ source('features_extra', 'leadlag_token_scores_daily') }} s
    ON s.token = b.token_address
   AND s.run_date < DATE(b.price_timestamp)
  QUALIFY ROW_NUMBER() OVER (
    PARTITION BY b.token_address, b.price_timestamp
    ORDER BY s.run_date DESC
  ) = 1
),

-- leaders' current returns at this hour + leader count
a2_hourly AS (
  SELECT
    b.token_address,
    b.price_timestamp,
    COALESCE(AVG(SAFE_CAST(lb.cumret_24h AS FLOAT64)), 0.0)          AS leaders_ret_24h,
    COALESCE(AVG(SAFE_CAST(lb.mean_ret_72h AS FLOAT64) * 72.0), 0.0) AS leaders_ret_72h,
    COUNT(l.leader_token)                                            AS n_leaders,
    ANY_VALUE(e.lead_score_90d)                                     AS lead_score_90d
  FROM base b
  LEFT JOIN a2_effective e
    ON e.token_address = b.token_address
   AND e.price_timestamp = b.price_timestamp
  LEFT JOIN {{ source('features_extra', 'leadlag_daily') }} l
    ON l.run_date = e.run_date
   AND l.token = b.token_address
  LEFT JOIN base lb
    ON lb.token_address = l.leader_token
   AND lb.price_timestamp = b.price_timestamp
  GROUP BY b.token_address, b.price_timestamp
),

c_hourly AS (
  SELECT
    token AS token_address,
    price_timestamp,
    SAFE_CAST(levy_pv_168h AS FLOAT64) AS levy_pv_168h,
    SAFE_CAST(mom_vt_7vd AS FLOAT64)   AS mom_vt_7vd
  FROM {{ source('features_extra', 'tier_c_features_hourly') }}
)

SELECT
  b.*,
  -- A2 lead-lag network
  COALESCE(a2.leaders_ret_24h, 0.0)  AS leaders_ret_24h,
  COALESCE(a2.leaders_ret_72h, 0.0)  AS leaders_ret_72h,
  COALESCE(a2.n_leaders, 0)          AS n_leaders,
  COALESCE(a2.lead_score_90d, 0.0)   AS lead_score_90d,
  -- Tier C
  c.levy_pv_168h,
  c.mom_vt_7vd
FROM base b
LEFT JOIN a2_hourly a2
  ON a2.token_address = b.token_address
 AND a2.price_timestamp = b.price_timestamp
LEFT JOIN c_hourly c
  ON c.token_address = b.token_address
 AND c.price_timestamp = b.price_timestamp
