{{ config(
  materialized='table',
  partition_by={
    'field': 'week_start',
    'data_type': 'date',
    'granularity': 'day'
  },
  cluster_by=['token_address']
) }}

-- Off-class assets a crypto-momentum policy has no edge on and should not hold:
-- liquid-staking-SOL derivatives (below) + tokenized equities (any 'Xs*' mint).
-- The live shadow (combo-s2) was filling its book with these (~50% of buys / 100%
-- of the current book) at cumret_7d ~-1% / drawdown_7d ~-5% -- the opposite of the
-- training profile (+6.8% / -10.6%). Applied as direct predicates at the same
-- NEW-week gates as scam_tokens (frozen history untouched); NOT via scam_tokens,
-- because a computed set referencing `assets` inside the correlated NOT EXISTS
-- can't be de-correlated by BigQuery. Validated 2026-08-04: excludes exactly the
-- 20 off-class members (10 LST + 10 xStock), 0 of the 101 legit tokens.
-- 2026-08-11: the curated address list below is a SAFETY NET only; the primary
-- LST gate is now the property-based `wrapped_major_tokens` CTE, which catches
-- new/unlisted wrappers the address list misses (vSOL leaked in and combo-s2
-- bought it 2026-08-10). 2026-08-21: that CTE was SOL-only, so wrapped BTC/ETH
-- sailed through -- it now tests SOL, BTC and ETH, and `pegged_tokens` covers
-- stablecoins and tokenized commodities. xStocks stay pattern-gated (Xs*).
-- LST mints below, in order: BNSOL, JitoSOL, JupSOL, PSOL, STKESOL, bSOL, bbSOL,
-- hyloSOL, mSOL, sSOL. (Keep the list address-only; Jinja parses tag delimiters
-- even inside SQL comments, so no dbt tag delimiters or per-line comments here.)
{% set off_class_lst_addresses = [
  'BNso1VUJnh4zcfpZa6986Ea66P6TCp59hvtNJ8b1X85',
  'J1toso1uCk3RLmjorhTtrVwY9HJ7X8V9yYac6Y7kGCPn',
  'jupSoLaHXQiZZTSfEWMTRRgpnyFm8f6sZdosWBjx93v',
  'pSo1f9nQXWgXibFtKf7NWYxb5enAM4qfP6UJSiXRQfL',
  'stke7uu3fXHsGqKVVjKnkmj65LRPVrqr4bLG2SJg7rh',
  'bSo13r4TkiE4KumL71LsHTPpL2euBYLFx6h9HP3piy1',
  'Bybit2vBJGhPF52GBdNaQfUJ6ZpThSgHBobjWZpLPb4B',
  'hy1oXYgrBW6PVcJ4s6s2FKavRdwgWTXdfE69AxT7kPT',
  'mSoLzYCxHdYgdzU16g5QSh3i5K3z3KZK7ytfqcJm7So',
  'sSo14endRuUbvQaJS3dq36Q829a3A6BEfoeeRGJywEh'
] %}

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

-- The week the SOL/BTC/ETH-wrapper and pegged-asset detectors take effect.
-- Weeks BEFORE this keep the membership they were computed with, for two reasons.
--
-- Provenance: weeks 2026-07-13..2026-08-10 were already traded against. Silently
-- rewriting which tokens were members in a week the book has already acted on
-- makes every past decision irreproducible against its own universe.
--
-- Exitability, which is the load-bearing one: `rl_prod_inference_features_v` is
-- members_only, so a token that is not a member of the CURRENT week disappears
-- from the live feed. The runner steps only what the feed contains, so a position
-- held in a vanished token is never stepped, never sold, and strands in the
-- ledger. The serving view retains a token for 2 weeks after its last member week
-- so an open position can still be closed -- but that only works if the token was
-- a member RECENTLY. Applying these detectors retroactively would push the last
-- member week of all nine back to the frozen boundary, defeating the retention
-- and stranding the three positions the book holds right now (INF, LBTC, and a
-- stablecoin -- all three off-class, which is the whole point).
--
-- So: excluded from this week forward, still visible long enough to be sold.
{% set off_class_effective_from = '2026-08-17' %}

-- Wash-traded / fake-market-cap tokens (2026-10-05). The OHLCV scam detectors
-- (scam_h_union) and the volume/mcap copycat screen in scam_tokens both miss a
-- token whose fake volume is STEADY and small next to a fake market cap; the
-- Amihud liquidity gate even rewards it, since wash volume barely moves price.
-- The live F-003 book bought one (MUSK, 2026-10-04). Identified from Birdeye's
-- market snapshot by activity per participant: fewer than 100 wallets behind at
-- least 250k USD of 24h volume, OR fewer than 2000 holders behind at least 50M
-- USD of market cap; applied to non-off-class members, it flags exactly these four
-- and no legitimate member (validated 2026-10-05 against all 76 current members):
-- MUSK TheMuskToken (33 wallets, 10k trades/24h, 743 holders, 316M mcap),
-- WYT WowMyToken (1737 holders, 85-272M mcap), CTM c8ntinuum (26-109 wallets,
-- ~1800 holders, 66-71M mcap), RIV RIV Coin (52-77 wallets, wallet leg only).
-- A curated address list, like off_class_lst_addresses, because the snapshot has
-- no timestamps and would make rebuilds non-deterministic. FORWARD-ONLY from
-- wash_effective_from: weeks already traded against keep their membership, and
-- the serving view's 2-week retention keeps a held token sellable after it exits.
{% set wash_traded_addresses = [
  'D4BPL1zvhhJbxUdgi2qVUtjx4jeQWyUr2PAUjKc9rN5x',
  '7pKXpFsnZS5BB4Eydk3uZ84FeKDSvkv1z4Hv5ayQ28RV',
  'C8fU5GdfAt5mnw2RK7HE6XJGFNxHpaskZMkXxdm88888',
  '2bpT3ksMdwdZ6DuHyq3FDUr7HDwvZ5DRZoT1fUPALJaH'
] %}
{% set wash_effective_from = '2026-10-12' %}

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
    dollar_vol_24h,
    price
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
),

-- Property-based detection of assets a crypto-momentum policy has no edge on.
-- Two independent mechanisms; a token is off-class if EITHER fires.
--
-- (1) wrapped_major_tokens -- price is a near-constant multiple of a major.
-- A wrapper's USD price is its underlying times a slowly-drifting factor (a
-- staking exchange rate, or 1.0 for a custodial wrapper), so LN(price / ref) has
-- almost no variance while a real token has a lot. Originally SOL-only, which
-- caught the LSTs and wSOL but NOT the four wrapped-BTC members (cbBTC, WBTC,
-- zBTC, LBTC) or WETH -- those track BTC and ETH, not SOL, so the SOL ratio test
-- sees ordinary variance and waves them through. LBTC was bought by the live
-- book on 2026-08-20. Now evaluated against three references.
--   Validated 2026-08-21 over this model's own `assets` window (from
--   frozen_through - 30d), flagging exactly 9 of the 99 members of week
--   2026-08-17, 0 legit false positives:
--     BTC  WBTC 0.00096 / cbBTC 0.00105 / LBTC 0.00574 / zBTC 0.00648 (all ~1.00x)
--     ETH  WETH 0.0 (self-reference, mean 1.00)
--     SOL  wSOL 0.0, INF 0.00488 (1.43x)
--   The nearest unflagged token inside the mean-ratio band is >2x the widest
--   flagged std. Tokens outside the band are not close on ratio_std either.
--
-- The BTC reference is `btcusdt`, which `assets` deliberately excludes, so it is
-- read from the source view under the SAME PIT bound -- no CURRENT_TIMESTAMP, so
-- the model stays deterministic. SOL and ETH are referenced by mint from `assets`
-- itself. A reference is matched by its own test (ratio std 0, mean 1.0), which
-- is how wSOL and WETH exclude themselves -- deliberate, and the reason no
-- separate address entry is needed for either.
--
-- NOTE this cannot reach a FRACTIONAL wrapper (e.g. a satoshi-denominated token
-- at ~1e-8 BTC): the mean-ratio band [0.5, 3.0] excludes it by construction. The
-- band is the guard against a coincidentally-correlated real token, so widening
-- it to catch fractional wrappers would trade a real false-positive risk for a
-- case that has never been a member. Curated addresses remain the safety net.
wrapped_major_tokens AS (
  SELECT DISTINCT token_address
  FROM (
    SELECT
      a.token_address,
      COUNT(*) AS n_bars,
      STDDEV(LN(a.price / r.ref_price)) AS log_ratio_std,
      AVG(a.price / r.ref_price) AS mean_ratio
    FROM assets a
    JOIN (
      SELECT 'SOL' AS ref_key, price_timestamp, price AS ref_price
      FROM assets
      WHERE token_address = 'So11111111111111111111111111111111111111112'
        AND price > 0
      UNION ALL
      SELECT 'ETH' AS ref_key, price_timestamp, price AS ref_price
      FROM assets
      WHERE token_address = '7vfCXTUXx5WJV5JADk17DUJ4ksgau7utNKj4b963voxs'
        AND price > 0
      UNION ALL
      SELECT 'BTC' AS ref_key, price_timestamp, price AS ref_price
      FROM {{ ref('rl_prod_asset_features_history_v') }}
      WHERE token_address = 'btcusdt'
        AND price > 0
        AND price_timestamp >= TIMESTAMP_SUB(
          TIMESTAMP(DATE '{{ frozen_through }}'), INTERVAL 30 DAY
        )
    ) r USING (price_timestamp)
    WHERE a.price > 0
    -- Per (token, reference). Grouping by token alone would pool all three
    -- references into one stddev and measure nothing.
    GROUP BY a.token_address, r.ref_key
  )
  WHERE n_bars >= 200
    AND log_ratio_std < 0.01
    AND mean_ratio BETWEEN 0.5 AND 3.0
),

-- (2) pegged_tokens -- realised volatility far below any real token.
-- The ratio test above cannot see a stablecoin or a tokenized commodity: they
-- track something with no crypto beta, so no crypto reference fits them. They
-- are instead obvious on absolute hourly volatility. Among week 2026-08-17's
-- members the ordered distribution over this window is
--     0.0065%  (a $1.00 stablecoin -- which the live book was HOLDING)
--     0.0219%  (tokenized gold, ~$4,110)
--     0.2081%  <- first real token
-- a ~10x gap, so 0.10% sits mid-gap. Measured over the SAME window as everything
-- else, which matters -- on a trailing 30d window the 0.2081% token reads 0.06%
-- (it simply had a quiet month) and a threshold tuned there would wrongly exclude
-- it.
--
-- RE-VALIDATED 2026-08-21 after the weekly wide backfill landed, which grew the
-- candidate pool from ~250 to ~1,170 tokens and therefore changed this CTE's
-- INPUT. The threshold survives, but the margin is narrower than the figures
-- above -- those were measured on the starved pool and must not be quoted as
-- current. On the full pool: highest flagged 0.0729% (a USD1 stablecoin), lowest
-- unflagged 0.1496% (a real $750 asset trading $2.7M/day). That is ~2x on each
-- side of 0.10%, not 10x. Nothing sits near the boundary, so the threshold holds
-- -- but it is now a 2x margin and should be re-checked if the pool widens again.
--
-- The wider pool also made this predicate MORE correct, not less. Of 206 tokens
-- flagged, 21 are entry-grade (>=$50k median volume AND >=$20M median mktcap) and
-- every one is genuinely off-class: USDC and USDT themselves, eight more ~$1.00
-- stablecoins, six YIELD-BEARING stables drifting slowly at 1.03-1.17, tokenized
-- gold, and the stablecoin the live book was holding. No momentum token appears
-- in that set. The remaining ~185 are dead tokens with 99%+ zero returns.
--
-- WHY A LIQUIDITY GATE MUST ACCOMPANY THIS: Birdeye records an untraded hour as a
-- zero-volume bar with the price CARRIED FORWARD, so an illiquid token
-- accumulates zero returns and its realised volatility is deflated toward this
-- threshold for reasons that have nothing to do with being pegged. Measured
-- 2026-08-21: members average 4.6% zero-volume bars, non-members 44.7%. This
-- predicate is safe only because it sits alongside the volume, mktcap and amihud
-- gates that keep the dead tail out on independent grounds.
-- Dead/frozen-price tokens also score ~0 here but are already non-members under
-- the volume and mktcap rules, so this predicate does not widen their exclusion.
pegged_tokens AS (
  SELECT token_address
  FROM (
    SELECT
      token_address,
      COUNT(*) AS n_bars,
      STDDEV(log_return) * 100 AS hourly_vol_pct
    FROM (
      -- LAG is analytic, so it must resolve in its own level before STDDEV
      -- aggregates over it.
      SELECT
        token_address,
        SAFE.LN(price / LAG(price) OVER (
          PARTITION BY token_address ORDER BY price_timestamp)) AS log_return
      FROM assets
      WHERE price > 0
    )
    GROUP BY token_address
  )
  WHERE n_bars >= 200
    AND hourly_vol_pct < 0.10
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
  -- Off-class exclusion (new weeks only): tokenized equities (Xs* pattern),
  -- curated LSTs (address list, kept as a safety net), property-detected
  -- wrappers of SOL/BTC/ETH, and pegged assets (stablecoins, tokenized gold).
  AND NOT STARTS_WITH(a.token_address, 'Xs')
  AND a.token_address NOT IN UNNEST({{ off_class_lst_addresses | tojson }})
  AND (
    w.week_start < DATE '{{ off_class_effective_from }}'
    OR NOT EXISTS (
      SELECT 1 FROM wrapped_major_tokens wm WHERE wm.token_address = a.token_address
    )
  )
  AND (
    w.week_start < DATE '{{ off_class_effective_from }}'
    OR NOT EXISTS (
      SELECT 1 FROM pegged_tokens pg WHERE pg.token_address = a.token_address
    )
  )
  AND (
    w.week_start < DATE '{{ wash_effective_from }}'
    OR a.token_address NOT IN UNNEST({{ wash_traded_addresses | tojson }})
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
  -- Off-class exclusion (new weeks only): tokenized equities (Xs* pattern) and
  -- curated LSTs (address list, safety net). The property-based wrapper/pegged
  -- detectors are applied in `eligible_token_weeks`, where a week is in scope.
  AND NOT STARTS_WITH(t.token_address, 'Xs')
  AND t.token_address NOT IN UNNEST({{ off_class_lst_addresses | tojson }})
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
    -- Wrapper/pegged exclusion applies from off_class_effective_from onward. It
    -- belongs here, not in `eligible_tokens`: that CTE is a per-TOKEN list with no
    -- week in scope, so a week-dependent predicate cannot be expressed there.
    AND (
      w.week_start < DATE '{{ off_class_effective_from }}'
      OR (
        NOT EXISTS (
          SELECT 1 FROM wrapped_major_tokens wm
          WHERE wm.token_address = e.token_address
        )
        AND NOT EXISTS (
          SELECT 1 FROM pegged_tokens pg
          WHERE pg.token_address = e.token_address
        )
      )
    )
    -- Wash-traded exclusion, forward-only from wash_effective_from (see top).
    AND (
      w.week_start < DATE '{{ wash_effective_from }}'
      OR e.token_address NOT IN UNNEST({{ wash_traded_addresses | tojson }})
    )
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
