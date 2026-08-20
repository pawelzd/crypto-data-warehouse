#!/usr/bin/env bash
# deploy_dbt_jobs.sh — build the dbt image and deploy the three rl_prod Cloud Run
# JOBS the weekly universe Workflow invokes (§2.3). One image, three jobs that
# differ only in DBT_COMMAND / DBT_SELECT. Auth is ADC (the runtime SA) — no key.
set -euo pipefail
cd "$(dirname "$0")/.."          # repo root

PROJECT="${PROJECT:-crypto-trading-474111}"
REGION="${REGION:-europe-central2}"
REPO="${REPO:-rl}"                                    # Artifact Registry repo
# PIN THE TAG. This defaulted to :latest, and on 2026-08-21 that mutable tag was
# the direct cause of a month-old universe running in production: someone built a
# fixed image on 08-14, pointed `dbt-run-rl-prod` at it by digest tag, and left
# `universe-membership-advance` on :latest from 08-06. The membership advance —
# the job that decides WHICH TOKENS THE BOOK MAY BUY — therefore ran pre-fix code
# every Monday, and nine wrapped/pegged assets stayed in the universe. The live
# book bought two of them. All three jobs must move together, on one tag you can
# name; `deploy/RELEASE_TAG` is that name.
DEPLOY_TAG="${DEPLOY_TAG:-$(cat "$(dirname "$0")/RELEASE_TAG" 2>/dev/null || echo latest)}"
IMAGE="${IMAGE:-${REGION}-docker.pkg.dev/${PROJECT}/${REPO}/crypto-data-warehouse:${DEPLOY_TAG}}"
# Runtime SA needs BigQuery data editor + job user on the target datasets.
RUNTIME_SA="${RUNTIME_SA:-rl-prod@${PROJECT}.iam.gserviceaccount.com}"

# The rl_prod serving-view selectors and the membership test selectors.
SELECT_MEMBERSHIP_ADVANCE="rl_prod_universe_membership_v1_state"
SELECT_REFRESH="rl_prod_universe_membership_pit rl_prod_asset_features_v rl_prod_inference_features_v"
SELECT_TESTS="rl_prod_membership_no_lookahead rl_prod_membership_hysteresis rl_prod_membership_incumbents_first rl_prod_membership_turnover rl_prod_membership_count_band rl_prod_no_scam_tokens rl_prod_universe_internal_consistency"

echo "[deploy] building dbt image -> $IMAGE (Cloud Build — no local Docker)"
gcloud builds submit --project "$PROJECT" --config cloudbuild.yaml \
  --substitutions="_DOCKERFILE=deploy/Dockerfile,_IMAGE=${IMAGE}" .

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

# Verify all three jobs actually landed on the SAME image. A job left behind on an
# older tag is exactly the failure this script just caused, and it is invisible
# until a weekly run silently applies stale rules.
echo "[deploy] verifying all three jobs are on $IMAGE"
_drift=0
for _j in universe-membership-advance dbt-run-rl-prod dbt-test-rl-prod; do
  _got="$(gcloud run jobs describe "$_j" --project "$PROJECT" --region "$REGION" \
    --format='value(spec.template.spec.template.spec.containers[0].image)' 2>/dev/null || true)"
  if [ "$_got" != "$IMAGE" ]; then
    echo "[deploy] DRIFT $_j is on '$_got', expected '$IMAGE'" >&2; _drift=1
  fi
done
if [ "$_drift" -ne 0 ]; then
  echo "[deploy] jobs disagree on image — fix before the next weekly run" >&2
  exit 1
fi
echo "[deploy] all three jobs on $IMAGE"

echo "[deploy] done. Three dbt jobs deployed; the universe-weekly Workflow "
echo "         (in rl-crypto/deploy/cloud) sequences them."
