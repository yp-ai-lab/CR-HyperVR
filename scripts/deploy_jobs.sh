#!/usr/bin/env bash
set -euo pipefail

PROJECT_ID=${PROJECT_ID:?set PROJECT_ID}
REGION=${REGION:-europe-west2}
AR_REPO=${AR_REPO:-embedding-service}
IMAGE=${IMAGE:-$REGION-docker.pkg.dev/$PROJECT_ID/$AR_REPO/api:latest}
JOBS_SA=${JOBS_SA:-embedding-jobs@${PROJECT_ID}.iam.gserviceaccount.com}
INSTANCE_CONNECTION_NAME=${INSTANCE_CONNECTION_NAME:-$(gcloud sql instances describe ${SQL_INSTANCE:-embeddings-sql-${REGION}} --format='value(connectionName)' 2>/dev/null || true)}

DATA_PREFIX=${DATA_PREFIX:-${GCS_DATA_BUCKET}/data}
PROCESSED_PREFIX=${PROCESSED_PREFIX:-${GCS_EMB_BUCKET}/data/processed}
TRIPLETS_PREFIX=${TRIPLETS_PREFIX:-${GCS_EMB_BUCKET}/triplets}
PROFILES_PATH=${PROFILES_PATH:-${GCS_EMB_BUCKET}/data/processed/user_profiles.parquet}
SERVICE_URL=${SERVICE_URL:-$(gcloud run services describe embedding-service --project "$PROJECT_ID" --region="$REGION" --format='value(status.url)' 2>/dev/null || true)}

echo "Image:        $IMAGE"
echo "Jobs SA:      $JOBS_SA"
echo "Cloud SQL:    ${INSTANCE_CONNECTION_NAME:-[unset]}"
echo "Data prefix:  ${DATA_PREFIX:-[unset]}"
echo "Processed:    ${PROCESSED_PREFIX:-[unset]}"
echo "Triplets:     ${TRIPLETS_PREFIX:-[unset]}"
echo "Profiles:     ${PROFILES_PATH:-[unset]}"
echo "Service URL:  ${SERVICE_URL:-[unset]}"

common_env=(
  --set-env-vars PYTHONPATH=/app
  --set-env-vars GCS_DATA_PREFIX=${DATA_PREFIX}
  --set-env-vars GCS_PROCESSED_PREFIX=${PROCESSED_PREFIX}
  --set-env-vars GCS_TRIPLETS_PREFIX=${TRIPLETS_PREFIX}
  --set-env-vars GCS_PROFILES_PATH=${PROFILES_PATH}
)

# Example job: run Phase 2 pipeline
gcloud run jobs deploy pipeline-phase2 \
  --image "$IMAGE" --project "$PROJECT_ID" --region "$REGION" \
  --service-account "$JOBS_SA" \
  --max-retries 1 --tasks 1 \
  --add-cloudsql-instances "${INSTANCE_CONNECTION_NAME}" \
  ${common_env[@]} \
  --command python --args scripts/run_pipeline_phase2.py

echo "Deployed Cloud Run Jobs: pipeline-phase2"

