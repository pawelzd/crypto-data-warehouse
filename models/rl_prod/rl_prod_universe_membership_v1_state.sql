{{ config(
  materialized='table',
  partition_by={
    'field': 'week_start',
    'data_type': 'date',
    'granularity': 'day'
  },
  cluster_by=['token_address']
) }}

-- Frozen v1 history plus weeks appended under the same v1 rules.
--
-- universe_membership_v1_snapshot_20260713 stops at week_start 2026-07-06 and
-- cannot advance, so every later week resolves to in_universe_pit = FALSE
-- downstream. This model copies the frozen range through byte-for-byte and
-- computes only the weeks after it, seeding the stateful streaks from the
-- snapshot's final week rather than recomputing the recursion from 2021.
--
-- Append-only is deliberate. Recomputing v1 in full from current OHLCV does not
-- reproduce the frozen range: 508 token-weeks across six tokens added to
-- scam_h_union after the freeze, and 53 token-weeks across ten tokens that now
-- clear the entry rule on backfilled history. That rebuild is better data, but
-- univ_* and rel_* are computed from member rows only, so adopting it shifts the
-- cross-sectional aggregates the deployed scaler and checkpoint were fit on.
-- Treat the full recompute as a retrain-boundary change, alongside the pending
-- v2 decision -- not as a maintenance refresh.
--
-- New weeks apply the current hardened scam list, so the six newly-excluded
-- tokens simply stop receiving rows. All six were already non-members at
-- 2026-07-06 with exit streaks of 9-83 weeks, so the boundary carries no
-- scam-driven exit event.
{% set frozen_through = '2026-07-06' %}

WITH RECURSIVE
frozen AS (
  SELECT
    token_address,
    first_observed_date,
    week_start,
    trailing_bar_count,
    median_mktcap_30d,
    median_dollar_vol_30d,
    dollar_volume_rank_30d,
    low_mktcap_streak,
    bad_volume_streak,
    meets_entry_rule,
    in_universe_pit,
    entered_this_week,
    exited_this_week,
    generated_at
  FROM {{ source('rl_prod_artifacts', 'universe_membership_v1_snapshot_20260713') }}
  WHERE week_start <= DATE '{{ frozen_through }}'
),

-- State carried into the first appended week.
seed AS (
  SELECT
    token_address,
    first_observed_date,
    low_mktcap_streak,
    bad_volume_streak,
    in_universe_pit
  FROM frozen
  WHERE week_start = DATE '{{ frozen_through }}'
),

-- Bounded to the trailing window the first appended week needs, so this stays a
-- recent-partition scan rather than a full-history one.
assets AS (
  SELECT
    token_address,
    price_timestamp,
    mktcap,
    dollar_vol_24h
  FROM {{ ref('rl_prod_asset_features_history_v') }}
  WHERE token_address != 'btcusdt'
    AND has_168h
    AND price_timestamp >= TIMESTAMP_SUB(
      TIMESTAMP(DATE '{{ frozen_through }}'), INTERVAL 30 DAY
    )
),

scam_tokens AS (
  SELECT DISTINCT token_address
  FROM {{ ref('scam_h_union') }}
  WHERE chain = 'sol'
  UNION DISTINCT
  -- Wash-traded copycats the OHLCV scam detector (scam_h_union) structurally MISSES:
  -- thin impersonators (fake HOOD/Robinhood) with little price history but a sustained
  -- 24h dollar-volume many multiples of market cap — a signature only the mcap/volume
  -- ratio shows. Computed from the SAME recent `assets` window the membership metrics
  -- use, so it stays deterministic (PIT-bounded to frozen_through, no CURRENT_TIMESTAMP)
  -- and consistent with scam_h_union. > 3x is the validated cutoff (2026-07-22: flags 0
  -- current members; highest member ratio 1.1). scam_tokens only gates NEW weeks, so the
  -- frozen range is copied from the snapshot untouched.
  SELECT token_address
  FROM (
    SELECT
      token_address,
      APPROX_QUANTILES(mktcap, 100)[OFFSET(50)] AS med_mktcap_30d,
      APPROX_QUANTILES(dollar_vol_24h, 100)[OFFSET(50)] AS med_dollar_vol_30d
    FROM assets
    GROUP BY token_address
  )
  WHERE med_mktcap_30d > 0 AND med_dollar_vol_30d > 3 * med_mktcap_30d
  UNION DISTINCT
  -- Off-class assets a crypto-momentum policy has no edge on and should not hold.
  -- The live shadow (combo-s2) was filling its book with these: ~50% of buys and
  -- 100% of the current 8-position book were LSTs/xStocks, entered at cumret_7d ~-1%
  -- / drawdown_7d ~-5% -- the opposite of the training profile (+6.8% / -10.6%
  -- momentum-on-pullback). They are liquid enough to clear the gates but pegged /
  -- flat, so there is no idiosyncratic momentum to capture. Validated 2026-08-04:
  -- flags exactly the 20 off-class members (10 + 10), 0 of the 101 legit tokens.
  -- Like the wash-copycat screen above, scam_tokens gates NEW weeks only -> the
  -- frozen training universe is copied from the snapshot untouched.
  --   * xStocks (tokenized equities): every mint uses the 'Xs' vanity prefix
  --     (10/10 Xs-prefixed members are genuine xStocks; no false positives).
  SELECT DISTINCT token_address
  FROM assets
  WHERE STARTS_WITH(token_address, 'Xs')
  UNION DISTINCT
  --   * LSTs (liquid-staking SOL derivatives): a stable curated set (~10 for months).
  --     Curated addresses rather than a symbol regex -- there is no symbol column here
  --     and `%SOL` would risk false-positives on memecoins. Add new LSTs here as they
  --     qualify (low-burden; the set has been stable).
  SELECT token_address FROM UNNEST([
    'BNso1VUJnh4zcfpZa6986Ea66P6TCp59hvtNJ8b1X85',   -- BNSOL
    'J1toso1uCk3RLmjorhTtrVwY9HJ7X8V9yYac6Y7kGCPn',  -- JitoSOL
    'jupSoLaHXQiZZTSfEWMTRRgpnyFm8f6sZdosWBjx93v',   -- JupSOL
    'pSo1f9nQXWgXibFtKf7NWYxb5enAM4qfP6UJSiXRQfL',   -- PSOL
    'stke7uu3fXHsGqKVVjKnkmj65LRPVrqr4bLG2SJg7rh',   -- STKESOL
    'bSo13r4TkiE4KumL71LsHTPpL2euBYLFx6h9HP3piy1',   -- bSOL
    'Bybit2vBJGhPF52GBdNaQfUJ6ZpThSgHBobjWZpLPb4B',  -- bbSOL
    'hy1oXYgrBW6PVcJ4s6s2FKavRdwgWTXdfE69AxT7kPT',   -- hyloSOL
    'mSoLzYCxHdYgdzU16g5QSh3i5K3z3KZK7ytfqcJm7So',   -- mSOL
    'sSo14endRuUbvQaJS3dq36Q829a3A6BEfoeeRGJywEh'    -- sSOL
  ]) AS token_address
),

new_weeks AS (
  SELECT
    week_start,
    ROW_NUMBER() OVER (ORDER BY week_start) AS new_week_number
  FROM (
    SELECT DATE_TRUNC(MAX(DATE(price_timestamp)), WEEK(MONDAY)) AS last_week
    FROM assets
  ),
  UNNEST(GENERATE_DATE_ARRAY(
    DATE_ADD(DATE '{{ frozen_through }}', INTERVAL 7 DAY),
    last_week,
    INTERVAL 7 DAY
  )) AS week_start
),

weekly_candidate_metrics AS (
  SELECT
    a.token_address,
    w.week_start,
    w.new_week_number,
    COUNT(*) AS trailing_bar_count,
    APPROX_QUANTILES(a.mktcap, 100)[OFFSET(50)] AS median_mktcap_30d,
    APPROX_QUANTILES(a.dollar_vol_24h, 100)[OFFSET(50)] AS median_dollar_vol_30d
  FROM new_weeks w
  INNER JOIN assets a
    ON a.price_timestamp >= TIMESTAMP_SUB(TIMESTAMP(w.week_start), INTERVAL 30 DAY)
   AND a.price_timestamp < TIMESTAMP(w.week_start)
  WHERE NOT EXISTS (
    SELECT 1
    FROM scam_tokens s
    WHERE s.token_address = a.token_address
  )
  GROUP BY a.token_address, w.week_start, w.new_week_number
),

ranked_candidates AS (
  SELECT
    m.*,
    CASE
      WHEN trailing_bar_count >= 500
        AND median_dollar_vol_30d IS NOT NULL
      THEN RANK() OVER (
        PARTITION BY week_start
        ORDER BY
          IF(trailing_bar_count >= 500, median_dollar_vol_30d, NULL) DESC NULLS LAST,
          token_address
      )
    END AS dollar_volume_rank_30d
  FROM weekly_candidate_metrics m
),

-- Incumbents from the snapshot plus any token newly observed after the freeze.
-- Tokens now on the scam list are dropped, matching the original eligibility gate.
eligible_tokens AS (
  SELECT
    t.token_address,
    COALESCE(t.first_observed_date, t.observed_first_date) AS first_observed_date
  FROM (
    SELECT
      COALESCE(s.token_address, n.token_address) AS token_address,
      s.first_observed_date,
      n.observed_first_date
    FROM seed s
    FULL OUTER JOIN (
      SELECT token_address, MIN(DATE(price_timestamp)) AS observed_first_date
      FROM assets
      GROUP BY token_address
    ) n
      ON n.token_address = s.token_address
  ) t
  WHERE NOT EXISTS (
    SELECT 1
    FROM scam_tokens sc
    WHERE sc.token_address = t.token_address
  )
),

eligible_token_weeks AS (
  SELECT
    e.token_address,
    e.first_observed_date,
    w.week_start,
    w.new_week_number
  FROM eligible_tokens e
  CROSS JOIN new_weeks w
  WHERE w.week_start >= e.first_observed_date
),

weekly_inputs AS (
  SELECT
    e.token_address,
    e.first_observed_date,
    e.week_start,
    e.new_week_number,
    COALESCE(r.trailing_bar_count, 0) AS trailing_bar_count,
    r.median_mktcap_30d,
    r.median_dollar_vol_30d,
    r.dollar_volume_rank_30d,
    COALESCE(r.median_mktcap_30d < 5000000, TRUE) AS below_exit_mktcap,
    COALESCE(r.dollar_volume_rank_30d > 250, TRUE) AS below_exit_volume,
    COALESCE(
      r.trailing_bar_count >= 500
      AND r.median_mktcap_30d >= 20000000
      AND r.dollar_volume_rank_30d <= 120,
      FALSE
    ) AS meets_entry_rule
  FROM eligible_token_weeks e
  LEFT JOIN ranked_candidates r
    ON r.token_address = e.token_address
   AND r.week_start = e.week_start
),

-- Seeding a token absent from the snapshot with streak 0 / in_universe_pit FALSE
-- reproduces the original token_week_number = 1 base case exactly.
membership_state AS (
  SELECT
    i.*,
    CAST(IF(i.below_exit_mktcap, COALESCE(p.low_mktcap_streak, 0) + 1, 0) AS INT64) AS low_mktcap_streak,
    CAST(IF(i.below_exit_volume, COALESCE(p.bad_volume_streak, 0) + 1, 0) AS INT64) AS bad_volume_streak,
    CASE
      WHEN COALESCE(p.in_universe_pit, FALSE) THEN NOT (
        (i.below_exit_mktcap AND COALESCE(p.low_mktcap_streak, 0) + 1 >= 2)
        OR (i.below_exit_volume AND COALESCE(p.bad_volume_streak, 0) + 1 >= 4)
      )
      ELSE i.meets_entry_rule
    END AS in_universe_pit
  FROM weekly_inputs i
  LEFT JOIN seed p
    ON p.token_address = i.token_address
  WHERE i.new_week_number = 1

  UNION ALL

  SELECT
    i.*,
    CAST(IF(i.below_exit_mktcap, p.low_mktcap_streak + 1, 0) AS INT64) AS low_mktcap_streak,
    CAST(IF(i.below_exit_volume, p.bad_volume_streak + 1, 0) AS INT64) AS bad_volume_streak,
    CASE
      WHEN p.in_universe_pit THEN NOT (
        (i.below_exit_mktcap AND p.low_mktcap_streak + 1 >= 2)
        OR (i.below_exit_volume AND p.bad_volume_streak + 1 >= 4)
      )
      ELSE i.meets_entry_rule
    END AS in_universe_pit
  FROM membership_state p
  INNER JOIN weekly_inputs i
    ON i.token_address = p.token_address
   AND i.new_week_number = p.new_week_number + 1
),

appended AS (
  SELECT
    s.token_address,
    s.first_observed_date,
    s.week_start,
    s.trailing_bar_count,
    s.median_mktcap_30d,
    s.median_dollar_vol_30d,
    s.dollar_volume_rank_30d,
    s.low_mktcap_streak,
    s.bad_volume_streak,
    s.meets_entry_rule,
    s.in_universe_pit,
    COALESCE(
      LAG(s.in_universe_pit) OVER (
        PARTITION BY s.token_address ORDER BY s.week_start
      ),
      sd.in_universe_pit,
      FALSE
    ) AS previous_in_universe
  FROM membership_state s
  LEFT JOIN seed sd
    ON sd.token_address = s.token_address
)

SELECT * FROM frozen

UNION ALL

SELECT
  token_address,
  first_observed_date,
  week_start,
  trailing_bar_count,
  median_mktcap_30d,
  median_dollar_vol_30d,
  dollar_volume_rank_30d,
  low_mktcap_streak,
  bad_volume_streak,
  meets_entry_rule,
  in_universe_pit,
  in_universe_pit AND NOT previous_in_universe AS entered_this_week,
  previous_in_universe AND NOT in_universe_pit AS exited_this_week,
  CURRENT_TIMESTAMP() AS generated_at
FROM appended
