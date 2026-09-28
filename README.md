# Project Kalinga: Azure-GCP BeamBridge 🚀

[![Spring Boot](https://img.shields.io/badge/Spring%20Boot-3.3-brightgreen.svg)](https://spring.io/projects/spring-boot)
[![Apache Beam](https://img.shields.io/badge/Apache%20Beam-Python%202.5x-orange.svg)](https://beam.apache.org/)
[![Google Cloud](https://img.shields.io/badge/Google%20Cloud-GKE%20%7C%20GCS%20%7C%20Dataflow%20%7C%20BigQuery-blue.svg)](https://cloud.google.com/)
[![Azure SQL](https://img.shields.io/badge/Source-Azure%20SQL%20Database-0078D4.svg)](https://azure.microsoft.com/en-us/products/azure-sql/)
[![PostgreSQL](https://img.shields.io/badge/Control%20DB-PostgreSQL%2016+-336791.svg)](https://www.postgresql.org/)

**Project Kalinga** is an enterprise-grade, horizontally scalable, multi-cloud ELT data pipeline engineered to ingest Extra Large (XL) retail datasets (5,000,000+ records) from **Microsoft Azure SQL Database** into **Google BigQuery** without Out-of-Memory (OOM) errors.

For the in-depth architectural blueprint, refer to [**`docs/ARCHITECTURE.md`**](docs/ARCHITECTURE.md).

---

## 🏗️ Architecture At A Glance

```
┌─────────────────┐       ┌────────────────────────┐       ┌──────────────────────┐
│  Azure SQL DB   │──────▶│ Spring Boot Extractor  │──────▶│ Google Cloud Storage │
│ (Source OLTP)   │       │ (GKE Pods - Keyset)    │       │ (Landing Lake JSONL) │
└─────────────────┘       └───────────┬────────────┘       └──────────┬───────────┘
                                      │                               │
                                      ▼                               ▼
                          ┌───────────────────────┐       ┌──────────────────────┐
                          │ PostgreSQL Control DB │       │ Cloud Dataflow (Beam)│
                          │ (Watermarks & State)  │       │ (Cleansing & Types)  │
                          └───────────────────────┘       └──────────┬───────────┘
                                                                      │
                                                                      ▼
                                                          ┌──────────────────────┐
                                                          │    Google BigQuery   │
                                                          │ (Partitioned/Cluster)│
                                                          └──────────────────────┘
```

1. **Control Layer**: PostgreSQL stores table-level extraction watermarks, chunk batch configurations, and status flags in `pipeline_control`.
2. **Extraction Layer**: A lightweight Spring Boot microservice running on Google Kubernetes Engine (GKE) coordinates multi-datasource routing, issues $O(1)$ B-Tree seek queries (keyset pagination) against Azure SQL, and streams records row-by-row into Google Cloud Storage (GCS).
3. **Transformation Layer**: A serverless Apache Beam (Python) pipeline on Cloud Dataflow reads partitioned JSONL files from GCS, cleanses timestamps and numeric types, diverts bad rows to a BigQuery Dead-Letter Queue (DLQ), and appends clean records into BigQuery.

---

## 📁 Repository Structure

```
.
├── docs/                                # Enterprise Documentation
│   └── ARCHITECTURE.md                  # Comprehensive Architecture Specification
├── extraction-service/                  # Spring Boot 3.3 Microservice (Java 21)
│   ├── pom.xml                          # Maven dependencies & build plugins
│   ├── Dockerfile                       # Multi-stage container build
│   ├── k8s/                             # Kubernetes manifests for GKE
│   │   ├── deployment.yaml              # Pod specs, resource limits, probes
│   │   ├── configmap.yaml               # Non-sensitive runtime configuration
│   │   └── secret.yaml.template         # Secret template for DB credentials
│   └── src/main/
│       ├── java/com/beambridge/extraction/
│       │   ├── config/                  # Dual DataSource Configs (PostgreSQL & Azure SQL)
│       │   ├── model/                   # PipelineControl entity & DTOs
│       │   ├── repository/              # Spring Data JPA Repository
│       │   └── service/                 # Keyset Pagination & GCS Stream Ingestion
│       └── resources/
│           ├── application.yml          # Production YAML config
│           └── application-local.yml    # Local development profile
├── transformation-pipeline/             # Apache Beam / Cloud Dataflow (Python)
│   ├── pipeline.py                      # Dataflow Pipeline Script (Cleansing & BigQuery Sink)
│   ├── requirements.txt                 # Python dependencies
│   ├── Dockerfile                       # Dataflow Flex Template Container
│   ├── run_local.sh                     # DirectRunner execution script
│   └── run_dataflow.sh                  # DataflowRunner submit script
├── Source_Data_Setup/                   # Synthetic Data & Verification Tooling
│   ├── TestDataCreation.py              # Generates categories, customers, orders, order_items CSVs
│   ├── postgres_connection.py           # Lightweight PostgreSQL connection helper
│   ├── test_postgres_connection.py      # Diagnostic CLI for PostgreSQL
│   ├── load_to_sql_server.py            # High-performance Azure SQL loader
│   └── .env.example                     # Local database configuration template
├── .env.example                         # Root environment variable template
├── .gitignore                           # Excludes credentials (*.env) and large CSVs
├── README.md                            # Developer Onboarding & Operations Guide
└── MEMORY.md                            # State tracker for Antigravity IDE sessions
```

---

## ⚙️ Prerequisites

Before getting started, ensure you have the following tools installed and authenticated:
- **Java JDK 21+** (for Spring Boot development)
- **Apache Maven 3.9+**
- **Python 3.10 to 3.12**
- **Docker Desktop / Engine**
- **Google Cloud SDK (`gcloud`)**
- **Kubernetes CLI (`kubectl`)**
- Access to:
  - Azure SQL Database (Firewall open for your dev machine / GKE NAT IP)
  - PostgreSQL Database (Local instance or Cloud SQL)
  - GCP Project with BigQuery, GCS, GKE, and Cloud Dataflow enabled

---

## 🔐 Environment Variables & Configuration

Create a `.env` file in the project root by copying `.env.example`:

```bash
cp .env.example .env
```

Edit `.env` with your actual connection details:

```env
# ==============================================================
# Azure SQL Database Configuration (Source OLTP)
# ==============================================================
DB_SERVER=your-server.database.windows.net
DB_PORT=1433
DB_NAME=YourAzureSqlDatabase
DB_TRUSTED_CONNECTION=no
DB_USER=your_azure_user
DB_PASSWORD=your_azure_password
DB_DRIVER=ODBC Driver 18 for SQL Server
DB_TRUST_SERVER_CERTIFICATE=yes

# ==============================================================
# PostgreSQL Database Configuration (Control Plane)
# ==============================================================
PG_HOST=localhost
PG_PORT=5432
PG_DATABASE=postgres
PG_USER=postgres
PG_PASSWORD=your_postgres_password
PG_SCHEMA=public
PG_SSLMODE=prefer

# ==============================================================
# Google Cloud Platform Configuration
# ==============================================================
GCP_PROJECT_ID=your-gcp-project-id
GCP_REGION=us-central1
GCS_LANDING_BUCKET=your-gcp-project-landing-zone
BIGQUERY_DATASET=retail_dw
BIGQUERY_DLQ_DATASET=retail_dlq
```

---

## 🚀 Local Development Walkthrough

### 1. Generate Source Datasets & Initialize Azure SQL
If you need to generate synthetic datasets (from 100 to 5,000,000 rows):

```powershell
# 1. Generate CSV flat files
python Source_Data_Setup/TestDataCreation.py

# 2. Ingest datasets into Azure SQL Server
python Source_Data_Setup/load_to_sql_server.py
```

### 2. Verify Control Database (PostgreSQL)
Ensure your local or Cloud SQL PostgreSQL service is running:

```powershell
python Source_Data_Setup/postgres_connection.py
```

Inspect the `pipeline_control` state:
```sql
SELECT * FROM pipeline_control;
```

| table_name | is_active | extract_type | watermark_column | last_watermark_value | chunk_size | gcs_landing_path |
| :--- | :---: | :--- | :--- | :---: | :---: | :--- |
| `categories` | `true` | `INCREMENTAL` | `category_id` | `0` | `50000` | `gs://<bucket>/retail/categories/` |
| `customers` | `true` | `INCREMENTAL` | `customer_id` | `0` | `50000` | `gs://<bucket>/retail/customers/` |
| `orders` | `true` | `INCREMENTAL` | `order_id` | `0` | `50000` | `gs://<bucket>/retail/orders/` |
| `order_items` | `true` | `INCREMENTAL` | `order_item_id` | `0` | `50000` | `gs://<bucket>/retail/order_items/` |

### 3. Run the Spring Boot Extraction Service
```powershell
cd extraction-service
mvn clean spring-boot:run -Dspring-boot.run.profiles=local
```
The service will:
- Query active tables from PostgreSQL `pipeline_control`.
- Extract data in chunks using keyset pagination (`WHERE id > watermark ORDER BY id ASC LIMIT chunk_size`).
- Stream records row-by-row into GCS landing buckets.
- Atomically advance the watermark in PostgreSQL.

### 4. Run Apache Beam Pipeline Locally (DirectRunner)
```powershell
cd transformation-pipeline
python -m venv venv
venv\Scripts\activate
pip install -r requirements.txt

python pipeline.py \
    --runner=DirectRunner \
    --project=your-gcp-project-id \
    --input_pattern="gs://your-landing-bucket/retail/orders/**/*.jsonl" \
    --table_name="orders" \
    --output_dataset="retail_dw" \
    --temp_location="gs://your-landing-bucket/temp"
```

---

## ☸️ Production Deployment to GKE

### 1. Build and Push Extraction Microservice Container
```bash
cd extraction-service

# Build container with Google Cloud Build or Docker
gcloud builds submit --tag gcr.io/${GCP_PROJECT_ID}/beambridge-extraction-service:1.0.0 .
```

### 2. Configure Kubernetes Secrets & Deploy to GKE
```bash
# Connect to your GKE cluster
gcloud container clusters get-credentials kalinga-cluster --region us-central1

# Create namespace
kubectl create namespace beambridge

# Create secret from environment variables
kubectl create secret generic beambridge-secrets \
    --namespace=beambridge \
    --from-literal=azure-db-user="${DB_USER}" \
    --from-literal=azure-db-password="${DB_PASSWORD}" \
    --from-literal=pg-db-user="${PG_USER}" \
    --from-literal=pg-db-password="${PG_PASSWORD}"

# Deploy configuration and pod specifications
kubectl apply -f k8s/configmap.yaml -n beambridge
kubectl apply -f k8s/deployment.yaml -n beambridge

# Verify pod status and memory utilization
kubectl get pods -n beambridge -w
kubectl top pods -n beambridge
```

---

## ⚡ Production Deployment to Cloud Dataflow

### Submit Distributed Dataflow Job
```bash
cd transformation-pipeline

python pipeline.py \
    --runner=DataflowRunner \
    --project=${GCP_PROJECT_ID} \
    --region=us-central1 \
    --temp_location="gs://${GCS_LANDING_BUCKET}/temp" \
    --staging_location="gs://${GCS_LANDING_BUCKET}/staging" \
    --input_pattern="gs://${GCS_LANDING_BUCKET}/retail/*/**/*.jsonl" \
    --output_dataset="retail_dw" \
    --dlq_dataset="retail_dlq" \
    --autoscaling_algorithm=THROUGHPUT_BASED \
    --max_num_workers=20 \
    --worker_machine_type=n2-standard-4 \
    --save_main_session=True
```

---

## 🛡️ Operational Runbook & FAQ

### Q1: How do I re-extract or backfill a table?
To reprocess data or backfill from a specific ID, update `last_watermark_value` in the PostgreSQL metadata table:
```sql
-- Reset orders extraction back to beginning
UPDATE pipeline_control 
SET last_watermark_value = 0, last_updated = NOW() 
WHERE table_name = 'orders';
```

### Q2: What happens if a pod crashes during a 50,000-row chunk extraction?
Because the PostgreSQL watermark is updated **only after** the GCS streaming channel is successfully closed and flushed, a restarted pod will cleanly pick up from the last successfully committed watermark. Object paths are deterministic (`chunk_watermark_<id>.jsonl`), ensuring idempotent rewrites without duplicate data.

### Q3: How do we prevent BigQuery schema mismatches?
The Apache Beam pipeline validates all records against strict table-specific Pydantic/dataclass models. Any malformed record is automatically redirected to the Dead-Letter Queue table (`retail_dlq.failed_records`) with the raw string payload and error stack trace for post-incident analysis.

---

## 📜 License
Distributed under the Apache 2.0 License. See `LICENSE` for details.
