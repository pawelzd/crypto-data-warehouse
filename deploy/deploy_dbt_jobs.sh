#!/usr/bin/env bash
# deploy_dbt_jobs.sh — build the dbt image and deploy the three rl_prod Cloud Run
# JOBS the weekly universe Workflow invokes (§2.3). One image, three jobs that
# differ only in DBT_COMMAND / DBT_SELECT. Auth is ADC (the runtime SA) — no key.
set -euo pipefail
cd "$(dirname "$0")/.."          # repo root

PROJECT="${PROJECT:-crypto-trading-474111}"
REGION="${REGION:-europe-central2}"
REPO="${REPO:-rl}"                                    # Artifact Registry repo
IMAGE="${IMAGE:-${REGION}-docker.pkg.dev/${PROJECT}/${REPO}/crypto-data-warehouse:latest}"
# Runtime SA needs BigQuery data editor + job user on the target datasets.
RUNTIME_SA="${RUNTIME_SA:-rl-dbt@${PROJECT}.iam.gserviceaccount.com}"

# The rl_prod serving-view selectors and the membership test selectors.
SELECT_MEMBERSHIP_ADVANCE="rl_prod_universe_membership_v1_state"
SELECT_REFRESH="rl_prod_universe_membership_pit rl_prod_asset_features_v rl_prod_inference_features_v"
SELECT_TESTS="rl_prod_membership_no_lookahead rl_prod_membership_hysteresis rl_prod_membership_incumbents_first rl_prod_membership_turnover rl_prod_membership_count_band rl_prod_no_scam_tokens rl_prod_universe_internal_consistency"

echo "[deploy] building dbt image -> $IMAGE"
docker build -f deploy/Dockerfile -t "$IMAGE" .
docker push "$IMAGE"

deploy_job() {                    # name, command, select
  local name="$1" cmd="$2" select="$3"
  echo "[deploy] job $name ($cmd)"
  gcloud run jobs deploy "$name" \
    --project "$PROJECT" --region "$REGION" \
    --image "$IMAGE" --service-account "$RUNTIME_SA" \
    --max-retries 1 --task-timeout 1800 \
    --set-env-vars "DBT_COMMAND=${cmd},DBT_SELECT=${select},DBT_TARGET=adc"
}

deploy_job "universe-membership-advance" "run"  "$SELECT_MEMBERSHIP_ADVANCE"
deploy_job "dbt-run-rl-prod"             "run"  "$SELECT_REFRESH"
deploy_job "dbt-test-rl-prod"            "test" "$SELECT_TESTS"

echo "[deploy] done. Three dbt jobs deployed; the universe-weekly Workflow "
echo "         (in rl-crypto/deploy/cloud) sequences them."
