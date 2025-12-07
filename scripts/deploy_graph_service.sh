#!/usr/bin/env bash
set -euo pipefail

: "${PROJECT_ID:?set PROJECT_ID}"
REGION=${REGION:-europe-west2}
SERVICE_NAME=${SERVICE_NAME:-infra-service}
AR_REPO=${AR_REPO:-embedding-service}
MODEL_GCS_URI=${MODEL_GCS_URI:-}

export CLOUDSDK_CONFIG="${CLOUDSDK_CONFIG:-$(pwd)/.gcloud}"
mkdir -p "$CLOUDSDK_CONFIG"

EXTRA_ARGS=(
  --set-env-vars USE_RERANKER=${USE_RERANKER:-false}
  --set-env-vars USE_GRAPH_SCORER=${USE_GRAPH_SCORER:-true}
)

if [[ -n "${EXTRA_SET_VARS:-}" ]]; then
  EXTRA_ARGS+=( --set-env-vars "${EXTRA_SET_VARS}" )
fi

if [[ -n "${EXTRA_FLAGS:-}" ]]; then
  # shellcheck disable=SC2206
  EXTRA_ARGS+=( ${EXTRA_FLAGS} )
fi

SERVICE_NAME=${SERVICE_NAME} PROJECT_ID=${PROJECT_ID} REGION=${REGION} AR_REPO=${AR_REPO} \
EXTRA_ARGS="${EXTRA_ARGS[*]}" MODEL_GCS_URI="${MODEL_GCS_URI}" \
bash "$(dirname "$0")/deploy_cloud_run.sh"

echo "Deployed Cloud Run service: ${SERVICE_NAME} (project=${PROJECT_ID}, region=${REGION})"

