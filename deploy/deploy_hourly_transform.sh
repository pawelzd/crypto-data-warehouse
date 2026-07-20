#!/usr/bin/env bash
# deploy_hourly_transform.sh — hourly Scheduler trigger for dbt-run-rl-prod (:08
# past the hour), refreshing rl_prod_asset_features_v + rl_prod_inference_features_v
# off the OHLCV the :02 ingest just landed. The job itself is deployed by
# deploy_dbt_jobs.sh; this only adds the hourly trigger.
#
# Chain: ingest :02 (data_top_up) -> transform :08 (here) -> decide :12 (rl-crypto).
# (Weekly membership advance stays on the universe-weekly Cloud Workflow, Mon 01:00.)
set -euo pipefail

PROJECT="${PROJECT:-crypto-trading-474111}"
REGION="${REGION:-europe-central2}"
JOB="${JOB:-dbt-run-rl-prod}"
SCHEDULER_SA="${SCHEDULER_SA:-rl-prod@${PROJECT}.iam.gserviceaccount.com}"

JOB_RUN_URI="https://${REGION}-run.googleapis.com/apis/run.googleapis.com/v1/namespaces/${PROJECT}/jobs/${JOB}:run"
gcloud scheduler jobs create http "${JOB}-hourly" \
  --project "$PROJECT" --location "$REGION" \
  --schedule "8 * * * *" --time-zone "Etc/UTC" \
  --uri "$JOB_RUN_URI" --http-method POST \
  --oauth-service-account-email "$SCHEDULER_SA" \
  --oauth-token-scope "https://www.googleapis.com/auth/cloud-platform" \
  || gcloud scheduler jobs update http "${JOB}-hourly" \
       --project "$PROJECT" --location "$REGION" --schedule "8 * * * *" --uri "$JOB_RUN_URI"

echo "[deploy] done. dbt-run-rl-prod runs hourly at :08 (feature refresh)."
