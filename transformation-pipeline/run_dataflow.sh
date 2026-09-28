#!/usr/bin/env bash
# ==============================================================
# Cloud Dataflow Submission Script: Apache Beam on GCP
# ==============================================================
set -euo pipefail

TABLE="${1:-orders}"
GCP_PROJECT="${GCP_PROJECT_ID:-your-gcp-project-id}"
REGION="${GCP_REGION:-us-central1}"
BUCKET="${GCS_LANDING_BUCKET:-your-gcp-project-landing-zone}"
JOB_NAME="beambridge-transform-${TABLE}-$(date +%Y%m%d-%H%M%S)"

echo "=========================================================="
echo "Submitting Cloud Dataflow Job (DataflowRunner)"
echo "Job Name:   ${JOB_NAME}"
echo "Table:      ${TABLE}"
echo "Project:    ${GCP_PROJECT}"
echo "Region:     ${REGION}"
echo "GCS Bucket: ${BUCKET}"
echo "=========================================================="

python pipeline.py \
    --runner=DataflowRunner \
    --project="${GCP_PROJECT}" \
    --region="${REGION}" \
    --job_name="${JOB_NAME}" \
    --input_pattern="gs://${BUCKET}/retail/${TABLE}/**/*.jsonl" \
    --table_name="${TABLE}" \
    --output_dataset="retail_dw" \
    --dlq_dataset="retail_dlq" \
    --temp_location="gs://${BUCKET}/temp" \
    --staging_location="gs://${BUCKET}/staging" \
    --autoscaling_algorithm=THROUGHPUT_BASED \
    --max_num_workers=20 \
    --worker_machine_type=n2-standard-4 \
    --save_main_session=True
