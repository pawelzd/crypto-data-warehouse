# crypto-data-warehouse — dbt Cloud Run Job image (§2.3).
#
# One image runs all three rl_prod dbt jobs (universe-membership-advance,
# dbt-run-rl-prod, dbt-test-rl-prod); they differ only in the DBT_COMMAND /
# DBT_SELECT env the Cloud Workflow sets. Auth is ADC via the runtime service
# account — no keyfile is baked in (the old SA keys were scrubbed from history).
#
# Build from the repo root:  docker build -f deploy/Dockerfile -t <IMAGE> .
FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    DBT_PROFILES_DIR=/app/.dbt

RUN apt-get update && apt-get install -y --no-install-recommends git ca-certificates \
    && rm -rf /var/lib/apt/lists/*

# Pin the adapter to the versions the project was validated with (dbt 1.12).
RUN pip install --no-cache-dir "dbt-bigquery==1.12.0"

WORKDIR /app
COPY . /app

# Pre-fetch dbt packages into the image so cold starts don't hit the network.
RUN dbt deps --profiles-dir .dbt || true

RUN chmod +x deploy/run_dbt.sh
ENTRYPOINT ["deploy/run_dbt.sh"]
