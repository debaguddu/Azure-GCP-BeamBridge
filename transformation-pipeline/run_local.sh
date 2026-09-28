#!/usr/bin/env bash
# ==============================================================
# Local Execution Script: DirectRunner for Apache Beam Pipeline
# ==============================================================
set -euo pipefail

TABLE="${1:-orders}"
GCP_PROJECT="${GCP_PROJECT_ID:-your-gcp-project-id}"
BUCKET="${GCS_LANDING_BUCKET:-your-gcp-project-landing-zone}"

echo "=========================================================="
echo "Running Apache Beam Pipeline Locally (DirectRunner)"
echo "Table:      ${TABLE}"
echo "Project:    ${GCP_PROJECT}"
echo "GCS Bucket: ${BUCKET}"
echo "=========================================================="

python pipeline.py \
    --runner=DirectRunner \
    --project="${GCP_PROJECT}" \
    --input_pattern="gs://${BUCKET}/retail/${TABLE}/**/*.jsonl" \
    --table_name="${TABLE}" \
    --output_dataset="retail_dw" \
    --dlq_dataset="retail_dlq" \
    --temp_location="gs://${BUCKET}/temp"
