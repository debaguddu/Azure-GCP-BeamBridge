"""
Project Kalinga: Apache Beam Transformation Pipeline
====================================================
Distributed ELT data cleansing, schema reconciliation, type coercion,
and BigQuery loading pipeline designed for Cloud Dataflow.

Consumes partitioned NDJSON/JSONL chunks from GCS, performs strict data validation,
diverts malformed records to a Dead-Letter Queue (DLQ), and appends clean records
to Google BigQuery.

Usage (Local DirectRunner):
    python pipeline.py \
        --runner=DirectRunner \
        --project=your-gcp-project \
        --input_pattern="gs://your-landing-bucket/retail/orders/**/*.jsonl" \
        --table_name=orders \
        --output_dataset=retail_dw \
        --temp_location="gs://your-landing-bucket/temp"

Usage (Cloud Dataflow):
    python pipeline.py \
        --runner=DataflowRunner \
        --project=your-gcp-project \
        --region=us-central1 \
        --input_pattern="gs://your-landing-bucket/retail/orders/**/*.jsonl" \
        --table_name=orders \
        --output_dataset=retail_dw \
        --temp_location="gs://your-landing-bucket/temp" \
        --staging_location="gs://your-landing-bucket/staging"
"""

import json
import logging
import argparse
from datetime import datetime, timezone
from decimal import Decimal
from typing import Dict, Any, Tuple

import apache_beam as beam
from apache_beam.options.pipeline_options import (
    PipelineOptions,
    GoogleCloudOptions,
    StandardOptions,
    SetupOptions,
)
from apache_beam.io.gcp.bigquery import BigQueryDisposition, WriteToBigQuery
from dateutil import parser as date_parser

# Tag identifiers for branching PCollections
TAG_CLEAN = "clean_records"
TAG_DLQ = "dlq_records"

# ==============================================================
# BIGQUERY TARGET TABLE SCHEMAS
# ==============================================================

TABLE_SCHEMAS: Dict[str, Dict[str, Any]] = {
    "categories": {
        "fields": [
            {"name": "category_id", "type": "INTEGER", "mode": "REQUIRED"},
            {"name": "category_name", "type": "STRING", "mode": "NULLABLE"},
            {"name": "department", "type": "STRING", "mode": "NULLABLE"},
            {"name": "ingestion_timestamp", "type": "TIMESTAMP", "mode": "REQUIRED"},
        ]
    },
    "customers": {
        "fields": [
            {"name": "customer_id", "type": "INTEGER", "mode": "REQUIRED"},
            {"name": "first_name", "type": "STRING", "mode": "NULLABLE"},
            {"name": "last_name", "type": "STRING", "mode": "NULLABLE"},
            {"name": "email", "type": "STRING", "mode": "NULLABLE"},
            {"name": "signup_date", "type": "DATE", "mode": "NULLABLE"},
            {"name": "city", "type": "STRING", "mode": "NULLABLE"},
            {"name": "ingestion_timestamp", "type": "TIMESTAMP", "mode": "REQUIRED"},
        ]
    },
    "orders": {
        "fields": [
            {"name": "order_id", "type": "INTEGER", "mode": "REQUIRED"},
            {"name": "customer_id", "type": "INTEGER", "mode": "NULLABLE"},
            {"name": "store_id", "type": "INTEGER", "mode": "NULLABLE"},
            {"name": "order_date", "type": "TIMESTAMP", "mode": "NULLABLE"},
            {"name": "total_amount", "type": "NUMERIC", "mode": "NULLABLE"},
            {"name": "status", "type": "STRING", "mode": "NULLABLE"},
            {"name": "ingestion_timestamp", "type": "TIMESTAMP", "mode": "REQUIRED"},
        ]
    },
    "order_items": {
        "fields": [
            {"name": "order_item_id", "type": "INTEGER", "mode": "REQUIRED"},
            {"name": "order_id", "type": "INTEGER", "mode": "REQUIRED"},
            {"name": "product_id", "type": "INTEGER", "mode": "NULLABLE"},
            {"name": "quantity", "type": "INTEGER", "mode": "NULLABLE"},
            {"name": "unit_price", "type": "NUMERIC", "mode": "NULLABLE"},
            {"name": "discount", "type": "NUMERIC", "mode": "NULLABLE"},
            {"name": "ingestion_timestamp", "type": "TIMESTAMP", "mode": "REQUIRED"},
        ]
    },
    "dlq_records": {
        "fields": [
            {"name": "table_name", "type": "STRING", "mode": "REQUIRED"},
            {"name": "raw_payload", "type": "STRING", "mode": "NULLABLE"},
            {"name": "error_reason", "type": "STRING", "mode": "REQUIRED"},
            {"name": "failed_at", "type": "TIMESTAMP", "mode": "REQUIRED"},
        ]
    },
}


# ==============================================================
# DATA CLEANSING & TRANSFORMATION DoFn
# ==============================================================

class CleanseAndValidateRecordDoFn(beam.DoFn):
    """
    Parses NDJSON strings, normalizes strings and timestamps,
    validates numeric constraints, and tags outputs into clean vs DLQ branches.
    """

    def __init__(self, table_name: str):
        self.table_name = table_name

    def process(self, line: str):
        if not line or not line.strip():
            return

        now_iso = datetime.now(timezone.utc).isoformat()

        try:
            record: Dict[str, Any] = json.loads(line.strip())
        except Exception as e:
            # Malformed JSON payload -> Route to DLQ
            yield beam.pvalue.TaggedOutput(
                TAG_DLQ,
                {
                    "table_name": self.table_name,
                    "raw_payload": line[:4000],
                    "error_reason": f"JSON Deserialization Failure: {str(e)}",
                    "failed_at": now_iso,
                },
            )
            return

        try:
            clean_record = self._transform_by_table(record, now_iso)
            yield beam.pvalue.TaggedOutput(TAG_CLEAN, clean_record)

        except Exception as e:
            # Value/Type constraint error -> Route to DLQ
            yield beam.pvalue.TaggedOutput(
                TAG_DLQ,
                {
                    "table_name": self.table_name,
                    "raw_payload": json.dumps(record)[:4000],
                    "error_reason": f"Validation/Coercion Failure: {str(e)}",
                    "failed_at": now_iso,
                },
            )

    def _transform_by_table(self, r: Dict[str, Any], now_iso: str) -> Dict[str, Any]:
        """Applies domain-specific cleansing and type casting rules."""
        out: Dict[str, Any] = {"ingestion_timestamp": now_iso}

        if self.table_name == "categories":
            out["category_id"] = int(r["category_id"])
            out["category_name"] = str(r.get("category_name", "")).strip() or None
            out["department"] = str(r.get("department", "")).strip() or None

        elif self.table_name == "customers":
            out["customer_id"] = int(r["customer_id"])
            out["first_name"] = str(r.get("first_name", "")).strip() or None
            out["last_name"] = str(r.get("last_name", "")).strip() or None
            out["email"] = str(r.get("email", "")).strip().lower() or None
            out["city"] = str(r.get("city", "")).strip() or None

            # Date Normalization (YYYY-MM-DD)
            signup_val = r.get("signup_date")
            if signup_val:
                dt = date_parser.parse(str(signup_val))
                out["signup_date"] = dt.strftime("%Y-%m-%d")
            else:
                out["signup_date"] = None

        elif self.table_name == "orders":
            out["order_id"] = int(r["order_id"])
            out["customer_id"] = int(r["customer_id"]) if r.get("customer_id") is not None else None
            out["store_id"] = int(r["store_id"]) if r.get("store_id") is not None else None

            # Timestamp Normalization (UTC ISO-8601)
            date_val = r.get("order_date")
            if date_val:
                dt = date_parser.parse(str(date_val))
                if dt.tzinfo is None:
                    dt = dt.replace(tzinfo=timezone.utc)
                out["order_date"] = dt.isoformat()
            else:
                out["order_date"] = None

            # Monetary amount validation (prevent negative totals)
            total = float(r.get("total_amount", 0.0))
            if total < 0:
                raise ValueError(f"Negative total_amount detected: {total}")
            out["total_amount"] = round(total, 2)

            out["status"] = str(r.get("status", "")).strip().upper() or "UNKNOWN"

        elif self.table_name == "order_items":
            out["order_item_id"] = int(r["order_item_id"])
            out["order_id"] = int(r["order_id"])
            out["product_id"] = int(r["product_id"]) if r.get("product_id") is not None else None

            qty = int(r.get("quantity", 0))
            if qty <= 0:
                raise ValueError(f"Non-positive quantity detected: {qty}")
            out["quantity"] = qty

            price = float(r.get("unit_price", 0.0))
            if price < 0:
                raise ValueError(f"Negative unit_price detected: {price}")
            out["unit_price"] = round(price, 2)

            disc = float(r.get("discount", 0.0))
            out["discount"] = max(0.0, round(disc, 2))

        else:
            # Generic passthrough for custom tables
            for k, v in r.items():
                out[k.lower()] = v

        return out


# ==============================================================
# PIPELINE ENTRY POINT & ORCHESTRATION
# ==============================================================

class BeamBridgePipelineOptions(PipelineOptions):
    @classmethod
    def _add_argparse_args(cls, parser):
        parser.add_argument(
            "--input_pattern",
            required=True,
            help="GCS input glob pattern for landed NDJSON files (e.g. gs://bucket/retail/orders/**/*.jsonl)",
        )
        parser.add_argument(
            "--table_name",
            required=True,
            choices=["categories", "customers", "orders", "order_items"],
            help="Target table name for schema selection and transformation logic",
        )
        parser.add_argument(
            "--output_dataset",
            default="retail_dw",
            help="Destination BigQuery dataset for clean analytical records",
        )
        parser.add_argument(
            "--dlq_dataset",
            default="retail_dlq",
            help="Destination BigQuery dataset for Dead-Letter Queue rejected records",
        )


def run():
    parser = argparse.ArgumentParser()
    known_args, pipeline_args = parser.parse_known_args()

    pipeline_options = PipelineOptions(pipeline_args)
    custom_options = pipeline_options.view_as(BeamBridgePipelineOptions)
    gcp_options = pipeline_options.view_as(GoogleCloudOptions)

    pipeline_options.view_as(SetupOptions).save_main_session = True

    table_name = custom_options.table_name
    input_pattern = custom_options.input_pattern
    output_dataset = custom_options.output_dataset
    dlq_dataset = custom_options.dlq_dataset
    project = gcp_options.project or "local-project"

    target_table_ref = f"{project}:{output_dataset}.{table_name}"
    dlq_table_ref = f"{project}:{dlq_dataset}.failed_records"

    logging.info(f"Initiating Apache Beam Pipeline for table: '{table_name}'")
    logging.info(f"Input GCS Pattern: {input_pattern}")
    logging.info(f"Target BigQuery Table: {target_table_ref}")
    logging.info(f"DLQ BigQuery Table:    {dlq_table_ref}")

    target_schema = TABLE_SCHEMAS.get(table_name, {"fields": []})
    dlq_schema = TABLE_SCHEMAS["dlq_records"]

    with beam.Pipeline(options=pipeline_options) as p:
        # Step 1: Read Newline Delimited JSON lines from GCS landing lake
        raw_lines = p | "ReadFromGCS" >> beam.io.ReadFromText(input_pattern)

        # Step 2: Cleanse, Validate, and Tag Records (Main vs DLQ Branch)
        transformed_branches = raw_lines | "CleanseAndValidate" >> beam.ParDo(
            CleanseAndValidateRecordDoFn(table_name)
        ).with_outputs(TAG_CLEAN, TAG_DLQ)

        clean_records = transformed_branches[TAG_CLEAN]
        dlq_records = transformed_branches[TAG_DLQ]

        # Step 3a: Write Cleansed Records to Target BigQuery Data Warehouse Table
        _ = clean_records | "WriteCleanToBigQuery" >> WriteToBigQuery(
            table=target_table_ref,
            schema=target_schema,
            write_disposition=BigQueryDisposition.WRITE_APPEND,
            create_disposition=BigQueryDisposition.CREATE_IF_NEEDED,
            method=WriteToBigQuery.Method.FILE_LOADS,
        )

        # Step 3b: Write Malformed Records to BigQuery Dead-Letter Queue
        _ = dlq_records | "WriteDLQToBigQuery" >> WriteToBigQuery(
            table=dlq_table_ref,
            schema=dlq_schema,
            write_disposition=BigQueryDisposition.WRITE_APPEND,
            create_disposition=BigQueryDisposition.CREATE_IF_NEEDED,
            method=WriteToBigQuery.Method.FILE_LOADS,
        )


if __name__ == "__main__":
    logging.getLogger().setLevel(logging.INFO)
    run()
