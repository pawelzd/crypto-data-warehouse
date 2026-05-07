{{ config(
    materialized = 'view'
) }}

-- Passthrough view: filter to rows with a full 168h lookback and rename
-- ts_hour → decision_ts for downstream compatibility.
--
-- All feature computation — including cross-asset features (BTC beta, alpha
-- return, cumulative spreads vs BTC/SOL, vol regime ratios) — now lives in
-- 20m_cv_prod_72_7d_before_rl. This model is a thin filter only.
SELECT
  * EXCEPT (ts_hour, has_168h),
  ts_hour  AS decision_ts,
  has_168h AS has_full_lookback
FROM {{ ref('20m_cv_prod_72_7d_before_rl') }}
WHERE has_168h = 1
ORDER BY token_address, ts_hour
