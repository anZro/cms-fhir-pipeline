# CMS BCDA FHIR Pipeline

An end-to-end bulk FHIR ingestion and analytics pipeline built on the [CMS Beneficiary Claims Data API (BCDA)](https://bcda.cms.gov/) sandbox — 10,000 synthetic Medicare enrollees, no approval required.

## Architecture

```
CMS BCDA API (sandbox)
      │  async bulk export (NDJSON)
      ▼
GCS Bronze  ─── raw NDJSON by resource type + transaction_time
      │  flatten + Parquet conversion (Python)
      ▼
GCS Silver  ─── Parquet by resource type
      │  BigQuery load job (WRITE_TRUNCATE)
      ▼
BigQuery (silver_patient, silver_coverage, silver_eob)
      │  dbt staging → intermediate → marts
      ▼
BigQuery Gold (dim_patient, fct_claims, fct_coverage)
      │  Evidence.dev
      ▼
Dashboard (population overview, claims analysis, coverage)
```

**Orchestration:** Apache Airflow (Astro CLI)  
**CI/CD:** GitHub Actions — dbt compile + test on every PR

## What This Demonstrates

- Bulk FHIR R4 async job pattern (kick off → poll → download)
- NDJSON streaming to GCS without loading into memory
- FHIR resource flattening (Patient, Coverage, ExplanationOfBenefit)
- Watermark-based incremental loads using BCDA's `_since` parameter
- Idempotent pipeline design — safe to re-run at any layer
- dbt staging → intermediate (array unnesting) → mart layer
- dbt-expectations data quality tests
- Airflow TaskGroup, XCom, per-task retry policy
- GitHub Actions CI against BigQuery

## Prerequisites

- Python 3.10+
- GCP account (free tier covers this project)
- [Astro CLI](https://docs.astronomer.io/astro/cli/install-cli) for Airflow
- Node.js 18+ for Evidence.dev

## Setup

```bash
# 1. Clone and enter the repo
git clone https://github.com/anZro/cms-fhir-pipeline.git
cd cms-fhir-pipeline

# 2. Create your .env from the example
cp .env.example .env
# Fill in GCP_PROJECT_ID, GCS_BUCKET, and point GOOGLE_APPLICATION_CREDENTIALS
# at your downloaded service-account.json

# 3. Install Python dependencies
pip install -r requirements.txt

# 4. Install dbt dependencies
cd dbt_fhir && dbt deps && cd ..
```

## Makefile Targets

| Command | What it does |
|---|---|
| `make ingest` | Auth → kick off BCDA export → poll → download NDJSON to GCS Bronze |
| `make transform` | Flatten NDJSON → Parquet → write to GCS Silver |
| `make load` | Load Silver Parquet from GCS into BigQuery |
| `make dbt-run` | Run all dbt models (staging → intermediate → marts) |
| `make dbt-test` | Run all dbt tests |
| `make all` | End to end |

## Airflow (Astro CLI)

```bash
astro dev start
# Open http://localhost:8080
# DAG: bcda_pipeline — scheduled @weekly
```

## Project Structure

```
cms-fhir-pipeline/
├── ingest/          # Auth, export, polling, download, BQ load, watermark
├── transform/       # FHIR flatteners: Patient, Coverage, EOB → Parquet
├── dbt_fhir/        # dbt project: staging → intermediate → marts
├── dags/            # Airflow DAG
└── .github/         # GitHub Actions CI
```

## Key Design Decisions

**Why async bulk export?** BCDA returns data for potentially millions of beneficiaries — a synchronous request would time out. Kick-off → poll → download is standard for bulk FHIR.

**Why NDJSON → Parquet before BigQuery?** Loading raw JSON into BigQuery is expensive and slow. Converting to columnar Parquet first reduces cost and load time significantly.

**Why store `diagnosis_json` as a raw string in Silver?** EOB diagnosis arrays can have 10–25 entries per claim. Flattening in Python creates a complex many-to-many structure; it's cleaner to let dbt handle unnesting in SQL where it's testable.

**Why write the watermark last?** If transform or dbt fails, the pipeline re-runs from the same `_since` point. Watermark advancement is the commit — only happens when the full run succeeds.
