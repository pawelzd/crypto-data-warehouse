#!/usr/bin/env bash
# run_dbt.sh — the Cloud Run Job entrypoint for every dbt job in this repo.
#
# One image, three jobs — they differ only in env:
#   universe-membership-advance : DBT_COMMAND=run  DBT_SELECT="rl_prod_universe_membership_v1_state"
#   dbt-run-rl-prod             : DBT_COMMAND=run  DBT_SELECT="rl_prod_universe_membership_pit rl_prod_asset_features_v rl_prod_inference_features_v"
#   dbt-test-rl-prod            : DBT_COMMAND=test DBT_SELECT="rl_prod_membership_no_lookahead rl_prod_membership_hysteresis rl_prod_membership_incumbents_first rl_prod_membership_turnover rl_prod_membership_count_band rl_prod_no_scam_tokens rl_prod_universe_internal_consistency"
#
# Auth is ADC (the Cloud Run runtime service account) via the `adc` profile
# target — no keyfile. Exit non-zero on failure so the calling Cloud Workflow
# branches to its fail-loud alert (never lets a bad refresh be "complete").
set -euo pipefail

DBT_COMMAND="${DBT_COMMAND:?set DBT_COMMAND=run|test}"
DBT_SELECT="${DBT_SELECT:?set DBT_SELECT to a space-separated selector list}"
DBT_TARGET="${DBT_TARGET:-adc}"

cd /app

# deps is idempotent + offline-cached in the image; re-run defensively so a job
# started from a cold layer still resolves dbt_utils.
dbt deps --profiles-dir .dbt >/dev/null

echo "[dbt] $DBT_COMMAND --select $DBT_SELECT (target=$DBT_TARGET)"
exec dbt "$DBT_COMMAND" --profiles-dir .dbt --target "$DBT_TARGET" \
  --select $DBT_SELECT --fail-fast
