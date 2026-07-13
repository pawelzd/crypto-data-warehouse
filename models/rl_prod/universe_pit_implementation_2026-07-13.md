# PIT universe implementation status — 2026-07-13

## Implemented

- `dollar_vol_24h` is sourced directly from hourly OHLCV inside the shared
  asset-feature macro and exposed for audit.
- `rl_prod_universe_membership_pit` evaluates weekly trailing-30d median market
  cap and dollar volume without future rows.
- Entry: median market cap at least $20M and candidate dollar-volume rank at
  most 120.
- Exit: two consecutive weeks below $5M or four consecutive weeks below rank
  250. Membership otherwise carries forward.
- Entry cannot precede the token's own observed OHLCV history and uses only the
  30 days strictly before each weekly decision.
- `univ_*` and `rel_*` are computed from `in_universe_pit = TRUE` rows only.
- History retains nonmember coverage/death context and exposes the membership
  flag. Live output contains members only.
- Death/event scam patterns are evidence, not lifetime filters. Born-bad
  lifetime filtering requires matched windows spanning more than 90 days.

## Scam audit

- Previous raw fixed list: 797 tokens.
- Tokens excluded only by death/event signatures and restored: 313.
- Hardened algorithmic lifetime exclusions: 220.
- Hardened union including manual exclusions: 221 unique source tokens.

## Eligibility source

CMC snapshot files are deliberately not used. The candidate set and its first
observable date come from the OHLCV-derived asset history. A token enters only
when its trailing metrics satisfy the entry rule, so rebuilding with later
history does not backdate membership.

Observed membership after the OHLCV-only rebuild:

- latest week (2026-07-06): 126 members;
- 2024 onward: 75–198 members, average 141.3;
- median weekly turnover from 2024 onward: 3.3%;
- latest complete live hour: 115 served members (the 90% ingestion-completeness
  guard suppresses later partial hours).

The 120–250 validation band passes from December 2024 onward. Earlier 2024
weeks fall as low as 75 and remain visible through the warning-severity audit;
tokens are not added merely to force a target count.

## 2025-12 ingestion boundary

The documented 185-token December 2025 cutoff is retained as source coverage,
not removed. PIT membership and the Amihud entry gate determine tradeability.
No additional 2026 backfill was attempted here; the existing OHLCV coverage is
retained and the weekly state determines tradeability.

The new `univ_n_active` distribution is not compatible with the old scaler or
checkpoint. Regenerate the scaler and fully retrain for this membership
definition.
