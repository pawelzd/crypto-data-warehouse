{{ config(
    materialized = 'table'
) }}

-- ============================================================================
-- §2 index helpers + idx variance for B3 herding (novel-features spec 2026-07-08).
-- Equal-weight meme-index return series over the active universe, one row per
-- price_timestamp. Materialized as its own model so it can feed BOTH the
-- per-token rl_info_structure_features table (A1 beta_idx / idio_mom_idx) and
-- the view (idx_* helpers, B3 herding) without a dependency cycle.
--
-- active_universe is replicated from the view's predicate (logret_1h present
-- AND price * circulating_supply > 0) so this index universe matches
-- rl_inference_features_next_open_v exactly. The index includes the token
-- itself (1/N effect at N>=100, accepted per §2 — no leave-one-out).
-- idx_cumret_* use SUM(idx_logret_1h) (cumulative LOG return) per the §2 recipe.
-- ============================================================================
WITH token_supply AS (
  SELECT
    address,
    MAX(SAFE_CAST(circulating_supply AS FLOAT64)) AS circulating_supply
  FROM {{ source('streamed_datapublic', 'public_tokens_to_monitor') }}
  GROUP BY address
),

universe AS (
  SELECT
    e.decision_ts AS price_timestamp,
    SAFE_CAST(e.logret_1h AS FLOAT64) AS logret_1h,
    SAFE_CAST(e.price AS FLOAT64) * s.circulating_supply AS mktcap_cost
  FROM {{ ref('20m_cv_prod_eval_dataset') }} e
  LEFT JOIN token_supply s
    ON e.token_address = s.address
),

idx_ts AS (
  SELECT
    price_timestamp,
    AVG(logret_1h) AS idx_logret_1h
  FROM universe
  WHERE logret_1h IS NOT NULL
    AND mktcap_cost IS NOT NULL
    AND mktcap_cost > 0
  GROUP BY price_timestamp
),

seq AS (
  SELECT
    price_timestamp,
    idx_logret_1h,
    ROW_NUMBER() OVER (ORDER BY price_timestamp) AS ts_idx
  FROM idx_ts
)

SELECT
  price_timestamp,
  CAST(idx_logret_1h AS FLOAT64) AS idx_logret_1h,
  CAST(IF(ts_idx >= 168, SUM(idx_logret_1h) OVER w168, NULL) AS FLOAT64) AS idx_cumret_7d,
  CAST(IF(ts_idx >= 720, SUM(idx_logret_1h) OVER w720, NULL) AS FLOAT64) AS idx_cumret_30d,
  -- idx return variance (single-series) for the B3 herding ratio numerator
  CAST(IF(ts_idx >= 168, VAR_POP(idx_logret_1h) OVER w168, NULL) AS FLOAT64) AS idx_var_168h,
  CAST(IF(ts_idx >= 720, VAR_POP(idx_logret_1h) OVER w720, NULL) AS FLOAT64) AS idx_var_720h
FROM seq
WINDOW
  w168 AS (ORDER BY price_timestamp ROWS BETWEEN 167 PRECEDING AND CURRENT ROW),
  w720 AS (ORDER BY price_timestamp ROWS BETWEEN 719 PRECEDING AND CURRENT ROW)
