-- Off-class assets (tokenized equities 'Xs*' and liquid-staking-SOL derivatives)
-- must not appear as live members. The scam_tokens exclusion in
-- rl_prod_universe_membership_v1_state gates NEW weeks only, so this is scoped to
-- post-freeze weeks (frozen history is copied from the snapshot untouched and may
-- still contain them). Returns rows (== test failures) if any post-freeze member
-- is an xStock or a known LST. Validated 2026-08-04: 10 xStock + 10 LST members
-- pre-fix; 0 legit tokens affected. Keep the LST list in sync with the model.
SELECT
  token_address,
  week_start
FROM {{ ref('rl_prod_universe_membership_v1_state') }}
WHERE in_universe_pit
  AND week_start > DATE '2026-07-13'   -- frozen_through: exclusion applies after
  AND (
    STARTS_WITH(token_address, 'Xs')                         -- xStocks (tokenized equities)
    OR token_address IN (                                    -- LSTs (staked-SOL derivatives)
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
