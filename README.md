# crypto-data-warehouse

The dbt/BigQuery warehouse for the crypto ML efforts — extracted from
`airflow-wallets/src/dbt_bq` (2026-07-20) into an orchestration-agnostic home so
`dbt run`/`dbt test` work the same whether triggered by Cloud Run, a laptop, or
Airflow. It is **not** RL-bot-specific: it serves both the RL trading bot
(`rl_prod.*` serving layer) and a separate tree-classifier ML effort
(`gold_ml_coins_mon`, EMA/Bollinger models, `20m_eval`).

Provenance: history-preserving `git subtree` split of `src/dbt_bq`. The
service-account key JSONs that were committed in the original repo were
**scrubbed from all history** during the move — this repo authenticates via
**Application Default Credentials only** (see `.dbt/profiles.yml`). Never commit
a keyfile here again (`.gitignore` blocks the known patterns).

## Layout

```
dbt_project.yml            profile: dbt_bq_profile  (project name: dbt_to_bq)
.dbt/profiles.yml          target: adc (BigQuery, method: oauth / ADC)
models/
  stg/ core_prices/ ...    staging → core
  rl_prod/                 the RL-bot serving layer (see below)
  ema/ gold_ml_coins_*/    the tree-classifier / strategy models
  scam_detection/          scam_h_*.sql — source of truth for scam flags
tests/                     rl_prod_membership_*, rl_prod_no_scam_tokens, ...
deploy/                    Cloud Run Job image + run/test entrypoint + deploy
```

### The `rl_prod` serving layer (what the RL bot reads)

- `rl_prod_universe_membership_v1_state` — the **v1** append-only carry-forward
  that advances membership past the frozen `universe_membership_v1_snapshot_20260713`
  (the fix for the stall where membership stopped at week 2026-07-06). v2 was
  built and **rejected** in acceptance testing; v1 is the deployed rule.
- `rl_prod_universe_membership_pit` — per-bar `in_universe_pit` off the v1 state.
- `rl_prod_asset_features_v` / `rl_prod_inference_features_v` — the feature store
  the RL decision service (`rl-crypto/deploy/shadow`) reads each hour.

## Run dbt locally

```bash
gcloud auth application-default login          # ADC — no keyfile
dbt deps  --profiles-dir .dbt
dbt build --profiles-dir .dbt --select rl_prod_universe_membership_v1_state
dbt test  --profiles-dir .dbt --select rl_prod_membership_no_lookahead rl_prod_no_scam_tokens
```

## Cloud Run jobs (§2.3 of the RL prod architecture)

One image (`deploy/Dockerfile`), three jobs the weekly universe Cloud Workflow
(defined in `rl-crypto/deploy/cloud/workflows/universe_weekly.yaml`) sequences —
they differ only in `DBT_COMMAND` / `DBT_SELECT`:

| job | command | selects |
|---|---|---|
| `universe-membership-advance` | `dbt run` | `rl_prod_universe_membership_v1_state` |
| `dbt-run-rl-prod` | `dbt run` | `rl_prod_universe_membership_pit rl_prod_asset_features_v rl_prod_inference_features_v` |
| `dbt-test-rl-prod` | `dbt test` | `rl_prod_membership_* rl_prod_no_scam_tokens rl_prod_universe_internal_consistency` |

Deploy all three:

```bash
PROJECT=crypto-trading-474111 deploy/deploy_dbt_jobs.sh
```

`run_dbt.sh` exits non-zero on failure so the Workflow branches to its fail-loud
alert and never lets a bad refresh be considered complete.

## Validated 2026-07-20 (from this new home)
`dbt deps` + `dbt parse` green, ADC connection OK, and every job/test selector
above resolves. `dbt run`/`dbt test` against the live warehouse need ADC
credentials with BigQuery access (they read/write `rl_prod`).
