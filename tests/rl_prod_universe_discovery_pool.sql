{{ config(severity='error') }}

-- Is the universe still able to ADMIT anyone?
--
-- Membership entry requires `trailing_bar_count >= 500` (~21 days of hourly bars).
-- Hourly ingestion is scoped to the ~100 tradable tokens for cost, so that history
-- comes from the WEEKLY wide backfill (birdeye-weekly-wide-backfill, run by the
-- universe-weekly Workflow between the candidate pull and membership_advance).
--
-- If that backfill silently stops, nothing breaks loudly. Existing members keep
-- their hourly bars, every job still exits 0, and the only symptom is that no new
-- token can ever qualify — so the universe ratchets down through normal hysteresis
-- exits, a few names a week. That is not hypothetical: hourly ingestion was
-- narrowed on 2026-07-21 and by 2026-08-21 the pool of tokens carrying a volume
-- rank had fallen 1,509 -> 454, dollar volume at rank #120 had fallen $113,873 ->
-- $2,991, 3,022 of 3,402 discovered candidates had no bar in the last 7 days, and
-- membership had drifted 111 -> 91. No test caught any of it: the count-band test
-- only fires below 20 members or on a >20% weekly contraction, and the turnover
-- test only looks at entries and exits, which stayed normal precisely BECAUSE
-- nothing was entering.
--
-- So this measures the INPUT to entry rather than its output: how many tokens
-- currently hold enough trailing history to be admitted at all. It is deliberately
-- a floor, not a band — a large pool is never a problem, an empty one is fatal.
--
-- Threshold: 200, from measurement rather than taste. On 2026-08-21 the first
-- full wide backfill (999 tokens, 537,065 candles) moved this pool from 114 to
-- 614. 114 is the starved state — barely more than the ~100 members themselves,
-- because only members were getting bars. 614 is healthy. 200 sits far enough
-- below 614 that ordinary week-to-week variation cannot fire it, and far enough
-- above 114 that a stalled backfill will.

WITH pool AS (
  SELECT token_address, COUNT(*) AS bars_30d
  FROM {{ ref('rl_prod_hourly_bars_history_v') }}
  WHERE price_timestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 30 DAY)
  GROUP BY token_address
  HAVING COUNT(*) >= 500
)
SELECT
  (SELECT COUNT(*) FROM pool) AS tokens_with_enough_history,
  200 AS required_floor,
  'weekly wide OHLCV backfill has probably stopped: too few tokens hold 500+ trailing hourly bars, so no new token can meet the membership entry rule and the universe will ratchet down' AS diagnosis
FROM (SELECT 1)
WHERE (SELECT COUNT(*) FROM pool) < 200
