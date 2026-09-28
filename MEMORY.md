# Project Kalinga: Session State & Architecture Memory

**File:** `MEMORY.md`  
**Last Updated:** 2026-09-28T18:45:00+05:30  
**Context:** Antigravity IDE Pair Programming Session  
**Roles:** Principal GCP Data Architect & Senior Enterprise Data Engineer  

---

## 1. Project Overview & Scope
- **Project Name:** Project Kalinga: Azure-GCP BeamBridge
- **Objective:** Build an enterprise-grade, horizontally scalable, multi-cloud ELT data pipeline capable of ingesting Extra Large (XL) retail datasets (5,000,000+ rows) from **Azure SQL Server** into **Google BigQuery** without Out-of-Memory (OOM) errors.
- **Architectural Tenet:** Full decoupling of Control Plane (PostgreSQL), Extraction Plane (Spring Boot on GKE), Landing Layer (GCS), and Distributed Transformation Plane (Apache Beam on Cloud Dataflow).

---

## 2. Technical Stack & Component Mapping

| Subsystem | Technology | Responsibility |
| :--- | :--- | :--- |
| **Source Store** | Azure SQL Server | Source OLTP retail database (`categories`, `customers`, `orders`, `order_items`) |
| **Control Store** | PostgreSQL 16+ | Pipeline metadata, watermarks, chunk configurations (`pipeline_control`) |
| **Extraction Service** | Spring Boot 3.3 (Java 21) | Keyset pagination extraction, multi-datasource routing, streaming directly to GCS |
| **Landing Zone** | Google Cloud Storage | Partitioned Newline-Delimited JSON (NDJSON/JSONL) immutable storage |
| **Transformation** | Apache Beam (Python 3.11+) | Cloud Dataflow job: data cleansing, timestamp coercion, schema validation, DLQ routing |
| **Analytical Sink** | Google BigQuery | Partitioned and clustered analytical data warehouse (`retail_dw`) |

---

## 3. Key Technical Decisions & Architectural Invariants

1. **Keyset Pagination (Seek Method) over Offset Pagination**:
   - Offset pagination (`OFFSET 4500000 FETCH NEXT 50000`) creates quadratic/linear $O(N)$ degradation and query timeouts on Azure SQL.
   - Keyset pagination (`WHERE watermark_column > last_watermark ORDER BY watermark_column ASC LIMIT chunk_size`) achieves deterministic $O(1)$ B-Tree direct index seeks in ~20ms regardless of dataset depth.

2. **$O(1)$ JVM Heap Discipline to Prevent GKE OOMKilled**:
   - Avoid buffering full resultsets into JVM memory.
   - Use `RowCallbackHandler` with `responseBuffering=adaptive` and `fetchSize=5000`.
   - Stream rows directly into Google Cloud Storage `WriteChannel` via `Channels.newOutputStream`.
   - Pods can run stably on 512MiB heap / 1GiB pod memory limits even for 5M+ row tables.

3. **Atomic Watermark Commit & Idempotency**:
   - The PostgreSQL `last_watermark_value` is updated **only after** the GCS stream has completed and closed.
   - Deterministic GCS file paths (`chunk_watermark_<id>.jsonl`) ensure that interrupted pod runs resume idempotently without duplicates.

4. **Dead-Letter Queue (DLQ) in Cloud Dataflow**:
   - Beam pipeline validates schema and types. Corrupted rows or malformed values are diverted to a separate BigQuery table (`retail_dlq.failed_records`) rather than failing the entire batch.

---

## 4. Work Breakdown & Execution Tracker

### Phase 1: Foundation & Connectivity
- [x] Test PostgreSQL connectivity and verify schema/tables (`test_postgres_connection.py`).
- [x] Identify active PostgreSQL 18 instance with `pipeline_control` table present.
- [x] Configure root and local `.env` and `.env.example` templates.
- [x] Protect sensitive credentials and large CSV datasets in `.gitignore`.
- [x] Initialize Git repository and push to GitHub (`https://github.com/debaguddu/Azure-GCP-BeamBridge`).

### Phase 2: Enterprise Scaffolding & Specifications
- [x] Establish Monorepo directory tree (`/docs`, `/extraction-service`, `/transformation-pipeline`).
- [x] Author comprehensive system blueprint: `docs/ARCHITECTURE.md`.
- [x] Author developer onboarding & deployment guide: `README.md`.
- [x] Initialize session state tracker: `MEMORY.md`.

### Phase 3: Extraction Service (`/extraction-service`)
- [x] Maven `pom.xml` with Spring Boot 3.3, Google Cloud Storage, HikariCP, MSSQL, PostgreSQL.
- [x] `application.yml` multi-datasource configuration for Azure SQL and PostgreSQL.
- [x] Dual DataSource configuration classes (`PostgresConfig.java`, `AzureSqlConfig.java`).
- [x] Control Plane Entity and JPA Repository (`PipelineControl.java`, `PipelineControlRepository.java`).
- [x] Keyset Pagination Extraction & GCS Streaming Service (`KeysetExtractionService.java`, `GcsStreamingService.java`).
- [x] Application entry point, REST controller & runner (`ExtractionServiceApplication.java`, `ExtractionController.java`, `ExtractionRunner.java`).
- [x] Multi-stage `Dockerfile` & Kubernetes manifests (`k8s/deployment.yaml`, `k8s/configmap.yaml`).

### Phase 4: Transformation Pipeline (`/transformation-pipeline`)
- [x] `requirements.txt` with Apache Beam, Google Cloud libraries, and Pydantic.
- [x] `pipeline.py` implementing Apache Beam distributed transformation, cleansing, and BigQuery IO sink.
- [x] Table schema definitions for BigQuery destination tables (`categories`, `customers`, `orders`, `order_items`).
- [x] Dead-Letter Queue (DLQ) tagging & BigQuery routing.
- [x] Local (`run_local.sh`), Dataflow (`run_dataflow.sh`) execution scripts, and Flex Template `Dockerfile`.

---

## 5. Notes & Observations
- Azure SQL contains: `categories` (100 rows), `customers` (50,000 rows), `orders` (1,000,000 rows), `order_items` (5,000,000 rows).
- PostgreSQL contains `pipeline_control` with active incremental watermark tracking for all 4 tables.
- Spring Boot extraction service is structured in standard clean architecture with zero-heap buffer streaming to GCS.
- Cloud Dataflow pipeline enforces strict schema casting, timestamp normalization, and DLQ diversion.

