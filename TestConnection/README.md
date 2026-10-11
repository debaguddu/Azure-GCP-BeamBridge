# Infrastructure Connectivity Test Suite (`TestConnection`)

This directory contains automated connection and health verification test scripts for all cloud and on-premises components integrated in the **GCP BeamBridge** pipeline.

## Prerequisites

Ensure all dependencies are installed in your Python environment:
```bash
pip install google-cloud-storage google-cloud-bigquery google-cloud-pubsub google-cloud-secret-manager google-cloud-kms pyodbc psycopg2-binary python-dotenv
```

Ensure your `.env` file exists at the root of the project with required credentials and connection strings.

## Test Scripts Overview

| Script | Service Tested | Key Validations |
| :--- | :--- | :--- |
| [`test_gcs.py`](./test_gcs.py) | **Google Cloud Storage (GCS)** | Authentication, landing bucket reachability, bucket metadata, object listing. |
| [`test_bigquery.py`](./test_bigquery.py) | **Google Cloud BigQuery** | Query execution (`CURRENT_TIMESTAMP`), dataset validation, dataset listing. |
| [`test_pubsub.py`](./test_pubsub.py) | **Google Cloud Pub/Sub** | Topic discovery, ingestion & DLQ topic check, subscription listing. |
| [`test_asql.py`](./test_asql.py) | **Azure SQL Server (ASQL)** | ODBC driver check, TCP/SQL authentication, DB version query, table inspection. |
| [`test_postgres.py`](./test_postgres.py) | **PostgreSQL** | TCP connection, database version check, schema table inspection. |
| [`test_secret_manager.py`](./test_secret_manager.py) | **GCP Secret Manager** | IAM permissions, listing secrets, payload retrieval and JSON parsing. |
| [`test_kms.py`](./test_kms.py) | **Google Cloud KMS** | KeyRing listing in target region, CryptoKey status and version state check. |
| [`run_all_tests.py`](./run_all_tests.py) | **Unified Suite Runner** | Sequentially executes all tests and displays an executive summary table. |

## Running the Tests

### Run All Tests Together
From the project root:
```bash
python TestConnection/run_all_tests.py
```

### Run Individual Tests
You can run any test script standalone:
```bash
python TestConnection/test_gcs.py
python TestConnection/test_bigquery.py
python TestConnection/test_pubsub.py
python TestConnection/test_asql.py
python TestConnection/test_postgres.py
python TestConnection/test_secret_manager.py
python TestConnection/test_kms.py
```
