-- The v1 snapshot was frozen to protect the deployed checkpoint's feature
-- contract. rl_prod_universe_membership_v1_state passes that range through
-- unchanged and only appends later weeks, so this test must hold trivially.
--
-- It is a regression guard, not an exploratory check: it fails the moment the
-- model starts recomputing frozen weeks instead of copying them. A full
-- recompute on today's OHLCV disagrees on 561 token-weeks (six tokens added to
-- scam_h_union after the freeze, ten that now clear the entry rule on
-- backfilled history), which shifts the univ_*/rel_* aggregates the deployed
-- scaler was fit on. Adopting that is a retrain-boundary decision.
{% set frozen_through = '2026-07-06' %}

WITH rebuilt AS (
  SELECT token_address, week_start, in_universe_pit
  FROM {{ ref('rl_prod_universe_membership_v1_state') }}
  WHERE week_start <= DATE '{{ frozen_through }}'
),

frozen AS (
  SELECT token_address, week_start, in_universe_pit
  FROM {{ source('rl_prod_artifacts', 'universe_membership_v1_snapshot_20260713') }}
  WHERE week_start <= DATE '{{ frozen_through }}'
)

SELECT
  COALESCE(r.token_address, f.token_address) AS token_address,
  COALESCE(r.week_start, f.week_start) AS week_start,
  r.in_universe_pit AS rebuilt_in_universe_pit,
  f.in_universe_pit AS frozen_in_universe_pit,
  CASE
    WHEN f.token_address IS NULL THEN 'only_in_rebuild'
    WHEN r.token_address IS NULL THEN 'only_in_snapshot'
    ELSE 'membership_disagrees'
  END AS failure_reason
FROM rebuilt r
FULL OUTER JOIN frozen f
  USING (token_address, week_start)
WHERE r.token_address IS NULL
   OR f.token_address IS NULL
   OR r.in_universe_pit != f.in_universe_pit
