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

# Unified structured-log line (same envelope as deploy/obs_log.py) via python3
# (present in the dbt image). Set OBS_SERVICE per job to distinguish the three.
OBS_SERVICE="${OBS_SERVICE:-dbt-rl-prod}"
log_json() {  # log_json EVENT LEVEL [key=value ...]
  OBS_SERVICE="$OBS_SERVICE" python3 - "$@" <<'PY'
import sys, os, json, datetime
ev = sys.argv[1] if len(sys.argv) > 1 else "event"
level = sys.argv[2] if len(sys.argv) > 2 else "INFO"
rec = {"ts": datetime.datetime.now(datetime.timezone.utc).isoformat(),
       "service": os.environ.get("OBS_SERVICE", "job"), "event": ev, "level": level}
for kv in sys.argv[3:]:
    k, _, v = kv.partition("=")
    try:
        v = json.loads(v)
    except Exception:
        pass
    rec[k] = v
print(json.dumps(rec, separators=(",", ":")))
PY
}

cd /app

# deps is idempotent + offline-cached in the image; re-run defensively so a job
# started from a cold layer still resolves dbt_utils.
dbt deps --profiles-dir .dbt >/dev/null

log_json cycle_start INFO "command=${DBT_COMMAND}" "select=${DBT_SELECT}" "target=${DBT_TARGET}"
# Run (not exec) so we can emit a structured cycle_done/cycle_error. `set -e`
# would abort on failure, so capture rc explicitly (still exit non-zero so the
# calling Workflow branches to its fail-loud alert).
if dbt "$DBT_COMMAND" --profiles-dir .dbt --target "$DBT_TARGET" \
  --select $DBT_SELECT --fail-fast; then rc=0; else rc=$?; fi

if [ "$rc" -eq 0 ]; then
  log_json cycle_done INFO "rc=0" "command=${DBT_COMMAND}"
else
  log_json cycle_error ERROR "rc=${rc}" "command=${DBT_COMMAND}"
fi
exit "$rc"
