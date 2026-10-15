# Cloud Data Lakehouse Pipeline

[![Python](https://img.shields.io/badge/Python-3.10%2B-3776AB.svg?logo=python&logoColor=white)](https://www.python.org/)
[![Apache Spark](https://img.shields.io/badge/Apache%20Spark-3.5.0-E25A1C.svg?logo=apachespark&logoColor=white)](https://spark.apache.org/)
[![Delta Lake](https://img.shields.io/badge/Delta%20Lake-3.2.0-00ADD8.svg?logo=delta&logoColor=white)](https://delta.io/)
[![AWS S3](https://img.shields.io/badge/Storage-AWS%20S3%20%2F%20MinIO-FF9900.svg?logo=amazons3&logoColor=white)](https://aws.amazon.com/s3/)
[![Trino](https://img.shields.io/badge/SQL%20Engine-Trino%20442-DD00A1.svg?logo=trino&logoColor=white)](https://trino.io/)
[![Pytest](https://img.shields.io/badge/Tests-8%20Passed-brightgreen.svg?logo=pytest&logoColor=white)](https://docs.pytest.org/)
[![Code Style: Black](https://img.shields.io/badge/code%20style-black-000000.svg)](https://github.com/psf/black)
[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](https://opensource.org/licenses/MIT)

**Author:** [Prabhat Sharma](https://github.com/prabhat84sharma-ui)  
**Profile:** [github.com/prabhat84sharma-ui](https://github.com/prabhat84sharma-ui)

---

## 📌 Executive Overview

An enterprise-grade, production-hardened **Cloud Data Lakehouse Pipeline** built with **PySpark 3.5**, **Delta Lake 3.2**, **AWS S3 / MinIO**, and **Trino**.

This project implements the industry-standard **Medallion Architecture (Bronze ➔ Silver ➔ Gold)** for streaming and batch transaction workloads. It solves key data engineering challenges:
- **Idempotency & Deduplication:** Handles out-of-order and duplicated transactions with ACID `MERGE` upserts.
- **Data Quality Gates & Quarantine:** Evaluates schema validity and business constraints with strict thresholds, routing corrupt records to a dead-letter quarantine table without crashing the pipeline.
- **Multi-Currency Normalization & Risk Scoring:** Standardizes international transactions into a canonical currency (USD) with dynamic tiering (`VIP`, `PREMIUM`, `STANDARD`) and fraud velocity alerts.
- **Advanced PySpark Window Functions:** Calculates running customer spend, inter-purchase duration (`LAG`), and customer lifetime value (`DENSE_RANK`).
- **Lakehouse Maintenance & Compaction:** Automates file compaction (`OPTIMIZE` with `Z-ORDER`) and garbage collection (`VACUUM`).

---

## 🏛 Architecture Diagram

```
                                  CLOUD DATA LAKEHOUSE PIPELINE
                                   (Medallion Architecture)

 +-----------------------------------------------------------------------------------------+
 |                                  INGESTION SOURCES                                      |
 |           CSV Files             |            JSON Feeds          |       Parquet Batches    |
 +-----------------------------------------------------------------------------------------+
                                                │
                                                ▼  (DataExtractor: Schema & Lineage Injected)
 +-----------------------------------------------------------------------------------------+
 |  BRONZE LAYER (Raw Ingestion / Append-Only)                                             |
 |  • Format: Delta Lake (Parquet + ACID Commit Log)                                       |
 |  • Storage: S3 / MinIO bucket: s3a://lakehouse-data/bronze/transactions/                |
 |  • Lineage Metadata: _ingestion_timestamp, _source_file, _batch_id                      |
 +-----------------------------------------------------------------------------------------+
                                                │
                                                ▼  (DataTransformer: Hygiene, Dedup, Quality Gate)
                   ┌────────────────────────────┴─────────────────────────────┐
                   │                                                          │
          [Passed Quality Gate]                                      [Failed Quality Gate]
                   │                                                          │
                   ▼                                                          ▼
 +-----------------------------------------+                +------------------------------------+
 |  SILVER LAYER (Cleaned & Conformed)     |                |  QUARANTINE / DEAD-LETTER QUEUE    |
 |  • Deduplicated by transaction_id       |                |  • Invalid PKs, negative amounts   |
 |  • Normalization: FX Currency to USD    |                |  • Reason: _quarantine_reason      |
 |  • ACID MERGE (Upsert) on Delta Table   |                |  • Preserved for investigation     |
 |  • Partitioned by: [region]             |                +------------------------------------+
 +-----------------------------------------+
                   │
                   ▼  (DataTransformer: Window Functions, Aggregations, Mart Generation)
 +-----------------------------------------------------------------------------------------+
 |  GOLD LAYER (Curated Business Data Marts)                                               |
 |                                                                                         |
 |  [Customer 360 Mart]                     [Regional Sales Mart]                          |
 |  • Metrics: Net Lifetime Spend           • Metrics: Daily Orders & Revenue              |
 |  • RFM Cadence & LTV Rank                • Metrics: Refund Rate & Failure Rate %        |
 |  • Dynamic Customer Tiering              • Partitioned by: [region]                     |
 +-----------------------------------------------------------------------------------------+
                                                │
                                                ▼  (Trino / Presto / Spark SQL / BI Dashboards)
 +-----------------------------------------------------------------------------------------+
 |  ANALYTICS & SERVING LAYER                                                              |
 |  • Trino 442 Distributed Query Engine (S3-backed Delta Lake catalog)                    |
 |  • Window Analytics: Rolling 7d/30d spend, RFM quartiles, velocity fraud detection      |
 |  • Delta Lake Time Travel: Historical audit and Change Data Capture (CDC) queries       |
 +-----------------------------------------------------------------------------------------+
```

---

## 🚀 Key Features

| Capability | Technical Implementation | Business Impact |
| :--- | :--- | :--- |
| **ACID Lakehouse Storage** | Delta Lake 3.2 commit logs with transactional serialization | Eliminates dirty reads and corrupted writes; allows concurrent reads/writes. |
| **Idempotent Upserts** | `DeltaTable.merge()` keyed on `transaction_id` with timestamp ordering | Prevents duplicate billing and guarantees exactly-once processing semantics. |
| **Data Quality Gate** | Multi-rule validation engine with automated pass/fail reports | Isolates corrupt payloads without aborting active ingestion pipelines. |
| **Window Analytics** | PySpark `Window.partitionBy()`, `rowsBetween()`, `rangeBetween()` | Calculates customer spending trajectory, inter-purchase velocity, and cohorts. |
| **Multi-Currency Normalization** | Configurable FX matrix translating EUR, GBP, CAD, INR to USD | Provides unified global executive reporting across international markets. |
| **File Compaction & Clustering** | `OPTIMIZE` with `ZORDER BY (customer_id, timestamp)` | Resolves small-file problem and accelerates query performance by up to 10x. |
| **Storage Portability** | S3A client adapter supporting both AWS S3 and local MinIO | Enables identical execution across local developer workstations and AWS EMR. |

---

## 📂 Project Directory Structure

```
cloud-data-lakehouse-pipeline/
├── .env.example                     # Environment template for local, MinIO, and AWS
├── .gitignore                       # Production gitignore for Spark, Delta, and Python
├── README.md                        # Architectural documentation and guide
├── docker-compose.yml               # Local infrastructure (MinIO S3 + Trino 442)
├── pyproject.toml                   # Project metadata, linters, and build config
├── requirements.txt                 # Pinned dependencies (PySpark 3.5, Delta Lake 3.2)
├── setup.py                         # Package setup script
│
├── data/
│   └── sample_transactions.csv      # Realistic multi-currency transaction dataset
│
├── docker/
│   └── trino/
│       └── catalog/
│           └── delta.properties     # Trino Delta Lake catalog pointing to MinIO S3
│
├── sql/
│   └── analytics_queries.sql        # 11 production-grade SQL analytics queries
│
├── src/
│   ├── __init__.py                  # Package init
│   ├── config.py                    # Typed configuration dataclasses & S3/Delta params
│   ├── extract.py                   # Multi-format extractor & automated data profiling
│   ├── transform.py                 # PySpark Medallion transforms, windowing, and marts
│   ├── load.py                      # Delta Lake loader, MERGE upsert, OPTIMIZE, VACUUM
│   ├── pipeline.py                  # End-to-end CLI orchestrator with timing & logging
│   └── utils.py                     # SparkSession factory, structured logging, reports
│
└── tests/
    └── test_transformations.py      # Pytest test suite for transformations & marts
```

---

## 🛠 Technology Stack

- **Compute & Processing:** [Apache Spark 3.5.0](https://spark.apache.org/) (PySpark)
- **Lakehouse Storage Format:** [Delta Lake 3.2.0](https://delta.io/)
- **Distributed Query Engine:** [Trino 442](https://trino.io/)
- **Object Storage:** [AWS S3](https://aws.amazon.com/s3/) / [MinIO](https://min.io/) (High-performance S3 compatible store)
- **Testing & Quality:** [Pytest](https://pytest.org/), [Black](https://black.readthedocs.io/), [Ruff](https://beta.ruff.rs/)
- **Language & Runtime:** Python 3.10+

---

## ⚙️ Quick Start & Setup Instructions

### 1. Prerequisites
- Python 3.10 or higher
- Java OpenJDK 11 or 17 (required for Apache Spark)
- Docker & Docker Compose (optional, for local MinIO & Trino)

### 2. Clone and Setup Environment
```bash
# Clone the repository
git clone https://github.com/prabhat84sharma-ui/cloud-data-lakehouse-pipeline.git
cd cloud-data-lakehouse-pipeline

# Create and activate a Python virtual environment
python -m venv .venv
source .venv/bin/activate  # On Windows: .venv\Scripts\activate

# Install dependencies in editable mode
pip install -r requirements.txt
pip install -e .
```

### 3. Configure Environment Variables
```bash
cp .env.example .env
# By default, ENVIRONMENT=local stores Delta tables in ./lakehouse_storage/
```

### 4. (Optional) Start Local MinIO & Trino Services
To simulate AWS S3 locally with MinIO and run Trino queries:
```bash
docker compose up -d
# MinIO S3 API: http://localhost:9000
# MinIO Console: http://localhost:9001 (User: minioadmin | Pass: minioadmin)
# Trino Web UI:  http://localhost:8080
```

---

## 🏃 Running the Pipeline

### Execute Full Medallion Pipeline (Bronze ➔ Silver ➔ Gold)
```bash
python -m src.pipeline --stage all
```

### Execute with Automated Delta Optimization & Vacuum Maintenance
```bash
python -m src.pipeline --stage all --optimize
```

### Execute Individual Stages
```bash
# Ingest raw source data into Bronze Delta table
python -m src.pipeline --stage bronze

# Run data hygiene, deduplication, and Silver upsert
python -m src.pipeline --stage silver

# Compute window analytics and refresh Gold data marts
python -m src.pipeline --stage gold
```

---

## 🧪 Running Unit Tests

Run the comprehensive pytest suite verifying data transformations, window metrics, and Gold aggregations:
```bash
pytest tests/ -v
```

Sample pytest output:
```
tests/test_transformations.py::test_clean_raw_data PASSED                [ 12%]
tests/test_transformations.py::test_deduplicate_records PASSED           [ 25%]
tests/test_transformations.py::test_enrich_currency_and_metrics PASSED  [ 37%]
tests/test_transformations.py::test_validate_quality_and_split PASSED    [ 50%]
tests/test_transformations.py::test_window_analytics PASSED             [ 62%]
tests/test_transformations.py::test_customer_360_gold_mart PASSED       [ 75%]
tests/test_transformations.py::test_regional_sales_gold_mart PASSED     [ 87%]
tests/test_transformations.py::test_delta_upsert_idempotency PASSED     [100%]

============================== 8 passed in 6.42s ==============================
```

---

## 📊 Sample Execution Output

```
================================================================================
  CLOUD DATA LAKEHOUSE PIPELINE (Medallion Architecture)
  Author: Prabhat Sharma (https://github.com/prabhat84sharma-ui)
  Engine: Apache Spark 3.5 | Delta Lake 3.2 | Storage: AWS S3 / MinIO
================================================================================

2024-03-03 14:10:02 [INFO] Initializing Cloud Data Lakehouse Pipeline...
2024-03-03 14:10:02 [INFO] SparkSession successfully provisioned. Version: 3.5.0
2024-03-03 14:10:03 [INFO] Initiating ingestion from [./data/sample_transactions.csv] | Format: [csv]
2024-03-03 14:10:04 [INFO] DATA PROFILING SUMMARY (Total Rows: 30)
----------------------------------------------------------------------
Column                    Type            Null Count   Null %    
----------------------------------------------------------------------
transaction_id            string          0            0.00%     
customer_id               string          0            0.00%     
product_id                string          0            0.00%     
amount                    double          0            0.00%     
currency                  string          0            0.00%     
timestamp                 string          0            0.00%     
region                    string          0            0.00%     
status                    string          0            0.00%     
----------------------------------------------------------------------
2024-03-03 14:10:05 [INFO] Bronze layer successfully written to [./lakehouse_storage/bronze/transactions].
2024-03-03 14:10:06 [INFO] Deduplication completed: 29 records retained, 1 duplicate records purged.
2024-03-03 14:10:06 [INFO] ================================================================================
2024-03-03 14:10:06 [INFO] DATA QUALITY REPORT: [SilverValidation] | Overall Status: [PASSED]
2024-03-03 14:10:06 [INFO] --------------------------------------------------------------------------------
2024-03-03 14:10:06 [INFO] Rule Name                 Status       Total      Passed     Failed     Pass %
2024-03-03 14:10:06 [INFO] --------------------------------------------------------------------------------
2024-03-03 14:10:06 [INFO] Non_Null_Transaction_ID   PASSED       29         29         0          100.00%
2024-03-03 14:10:06 [INFO] Positive_Amount           PASSED       29         28         1          96.55%
2024-03-03 14:10:06 [INFO] Valid_Status_Enum         PASSED       29         29         0          100.00%
2024-03-03 14:10:06 [INFO] Valid_Timestamp           PASSED       29         29         0          100.00%
2024-03-03 14:10:06 [INFO] ================================================================================
2024-03-03 14:10:06 [WARN] Writing 1 quarantined records to [./lakehouse_storage/quarantine/transactions]...
2024-03-03 14:10:07 [INFO] Delta MERGE upsert executed successfully.
2024-03-03 14:10:08 [INFO] Top Customers by LTV Rank:
+--------+-----------+------------------+-------------------+----------------+
|ltv_rank|customer_id|total_transactions|total_net_spend_usd|customer_segment|
+--------+-----------+------------------+-------------------+----------------+
|1       |CUST-015   |1                 |12500.0            |ENTERPRISE_VIP  |
|2       |CUST-009   |1                 |5400.0             |ENTERPRISE_VIP  |
|3       |CUST-007   |2                 |1664.0             |HIGH_VALUE      |
|4       |CUST-001   |3                 |1404.44            |HIGH_VALUE      |
|5       |CUST-002   |3                 |1331.21            |HIGH_VALUE      |
+--------+-----------+------------------+-------------------+----------------+
2024-03-03 14:10:09 [INFO] Regional Sales Performance Preview:
+----------+----------------+------------+----------------+---------------+----------------+
|region    |transaction_date|total_orders|completed_orders|refunded_orders|gross_revenue_usd|
+----------+----------------+------------+----------------+---------------+----------------+
|US-WEST   |2024-03-03      |2           |2               |0              |13350.0         |
|US-EAST   |2024-03-01      |4           |3               |0              |1379.94         |
|EU-WEST   |2024-03-01      |3           |3               |0              |1084.97         |
|AP-SOUTH  |2024-03-02      |1           |1               |0              |180.0           |
|CA-CENTRAL|2024-03-01      |1           |1               |0              |65.95           |
+----------+----------------+------------+----------------+---------------+----------------+
2024-03-03 14:10:10 [INFO] LAKEHOUSE PIPELINE COMPLETED SUCCESSFULLY IN 8.15 SECONDS!
```

---

## 💡 Advanced SQL Analytics Suite

The [`sql/analytics_queries.sql`](sql/analytics_queries.sql) file contains 11 production-grade, Trino/Spark SQL queries:
1. **Rolling 7-Day & 30-Day Customer Spend Matrix:** Sliding frame window aggregations.
2. **Customer RFM Segmentation:** Recency, Frequency, and Monetary scoring using `NTILE(4)`.
3. **High-Velocity Fraud Detection:** Identifies rapid-fire swiping patterns across regions in `< 5 minutes` with `LAG()`.
4. **Cohort Retention & Activity Matrix:** Tracks customer lifetime engagement by monthly acquisition cohorts.
5. **Regional Pareto (80/20 Rule) Analysis:** Cumulative revenue distribution using window functions.
6. **Month-over-Month (MoM) Growth Analysis:** Revenue acceleration tracking.
7. **Territory Top-N Key Accounts:** Resolves ties using `DENSE_RANK()`.
8. **Multi-Currency FX Risk & Exposure:** Portfolio foreign currency breakdown.
9. **Inter-Purchase Cadence & Churn Alert:** Flags accounts exceeding 2x their average transaction cadence.
10. **Delta Lake Time-Travel & CDC Audit:** Compares `VERSION AS OF 0` with current commit snapshot.
11. **Product Velocity & Refund Root Cause:** Pinpoints products causing anomalous refund rates.

---

## 🔒 Production Best Practices Implemented

- **Schema Evolution & Enforcement:** Delta Lake schema validation prevents accidental drift while allowing safe schema evolution.
- **ACID Transactions:** Eliminates partial writes or orphan files during pipeline failures.
- **Quarantine Pattern:** Malformed records are preserved with actionable diagnostic metadata (`_quarantine_reason`) rather than dropped silently.
- **Data Lineage:** Every row tracks its ingestion timestamp, source file URI, and batch execution ID.
- **Resource Cleanup:** Spark sessions and connections are closed deterministically in `finally` blocks.

---

## 📄 License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

---

## 👤 Author & Contact

**Prabhat Sharma**  
Senior Data Engineer | Lakehouse & Distributed Systems Specialist  
- **GitHub:** [@prabhat84sharma-ui](https://github.com/prabhat84sharma-ui)
- **LinkedIn:** [linkedin.com/in/prabhatsharma](https://linkedin.com)
