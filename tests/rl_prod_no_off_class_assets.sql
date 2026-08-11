-- Off-class assets (tokenized equities 'Xs*' and liquid-staking-SOL derivatives)
-- must not appear as live members. The exclusions in
-- rl_prod_universe_membership_v1_state gate NEW weeks only, so this is scoped to
-- post-freeze weeks (frozen history is copied from the snapshot untouched and may
-- still contain them). Returns rows (== test failures) for any post-freeze member
-- that is an xStock, a PROPERTY-detected SOL-tracking LST (2026-08-11; catches
-- new/unlisted LSTs like vSOL so the test doesn't silently pass on a stale list),
-- or a known LST. Validated 2026-08-04: 10 xStock + 10 LST members pre-fix; 0
-- legit tokens affected.
WITH sol AS (
  SELECT price_timestamp, price AS sol_price
  FROM {{ ref('rl_prod_asset_features_history_v') }}
  WHERE token_address = 'So11111111111111111111111111111111111111112' AND price > 0
    AND price_timestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 30 DAY)
),
sol_trackers AS (
  SELECT a.token_address
  FROM {{ ref('rl_prod_asset_features_history_v') }} a
  JOIN sol s USING (price_timestamp)
  WHERE a.price > 0 AND a.token_address != 'btcusdt'
    AND a.price_timestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 30 DAY)
  GROUP BY a.token_address
  HAVING COUNT(*) >= 200
     AND STDDEV(LN(a.price / s.sol_price)) < 0.01
     AND AVG(a.price / s.sol_price) BETWEEN 0.5 AND 3.0
)
SELECT
  token_address,
  week_start
FROM {{ ref('rl_prod_universe_membership_v1_state') }}
WHERE in_universe_pit
  AND week_start > DATE '2026-07-13'   -- frozen_through: exclusion applies after
  AND (
    STARTS_WITH(token_address, 'Xs')                         -- xStocks (tokenized equities)
    OR token_address IN (SELECT token_address FROM sol_trackers)  -- property-detected LSTs
    OR token_address IN (                                    -- known LSTs (safety net)
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
    )
  )
