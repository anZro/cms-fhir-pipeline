.PHONY: ingest transform load dbt-run dbt-test all

# Run full ingest: auth → export → poll → download to GCS Bronze
ingest:
	python -m ingest.run_ingest

# Run transform layer: NDJSON → Parquet → GCS Silver
transform:
	python -m transform.run_transform

# Load Silver Parquet from GCS into BigQuery
load:
	python -m ingest.load_bq

# Run all dbt models
dbt-run:
	cd dbt_fhir && dbt run

# Run all dbt tests
dbt-test:
	cd dbt_fhir && dbt test

# End to end: ingest → transform → load → dbt
all: ingest transform load dbt-run dbt-test
