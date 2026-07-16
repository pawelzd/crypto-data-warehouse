# Dynamic universe v2 — side-by-side acceptance status

Implementation is complete but production remains on the frozen v1 snapshot.
The explicit dbt variable `rl_prod_universe_version: v2` is required to switch
the serving relation at the associated retrain boundary.

## Build facts

- Shared config hash:
  `538178cf2c206b847d57f6213859842017740ef734c264402e7462b64bef625a`
- Weekly PIT inputs: 233,865 rows across 239 weeks.
- Zero cap-boundary evictions and zero weeks above 150 members.
- Zero born-bad member rows and zero exit-band violations.
- Every raw/feature/scam timestamp recorded on a row is strictly before its
  week boundary.

## Acceptance failure

The literal specified thresholds do not produce the expected high-overlap,
low-turnover universe:

- Since 2024, v2 ranges from 2 to 56 members; latest is 24 versus v1's 111.
- Median v1/v2 Jaccard overlap since 2024 is 0.140.
- Latest exposure is hard stand-down (`n_active < 40`).
- 2026-06-29 contracts 47 → 30 members; 2026-07-06 contracts 30 → 24.
- Weekly turnover exceeds 10% around synchronized four-week quality exits.

The contraction is driven primarily by the p90 spread rule. Median p90 spread
among otherwise size/continuity-eligible tokens rises above the configured
100 bps during recent weeks (about 118–132 bps), aging many incumbents into the
four-week quality exit together. Continuity is not the cause: its recent p90
longest gap is only 5–6 hours against the 24-hour limit.

Do not tune this using 2026. If the 100-bps spread threshold is reconsidered,
calibrate alternatives on pre-2026 only, freeze one, then rerun this untouched
2026 acceptance. Until that decision and a full retrain, v1 remains deployed.
