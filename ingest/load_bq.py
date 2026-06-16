import google.cloud.bigquery
import google.cloud.exceptions
import structlog
import os
from dotenv import load_dotenv
from google.cloud.bigquery import SchemaField

coverage_schema = [
    SchemaField("coverage_id", "STRING"),
    SchemaField("status", "STRING"),
    SchemaField("subscriber_id", "STRING"),
    SchemaField("patient_id", "STRING"),
    SchemaField("medicare_part", "STRING"),
    SchemaField("period_start", "STRING"),
    SchemaField("period_end", "STRING"),
    SchemaField("dual_eligible", "STRING"),
    SchemaField("ms_cd_code", "STRING"),
    SchemaField("ms_cd_display", "STRING"),
    SchemaField("termination_code", "STRING"),
    SchemaField("reference_year", "STRING"),
    SchemaField("last_updated", "STRING"),
]

load_dotenv()

logger = structlog.get_logger(__name__)

BQ_DATASET = os.getenv("BQ_DATASET")
GCS_BUCKET = os.getenv("GCS_BUCKET")
GCP_PROJECT_ID = os.getenv("GCP_PROJECT_ID")

def load_to_bigquery(bq_client, gcs_uri, table_id, project, dataset, schema=None) -> None:
    table_ref = f"{project}.{dataset}.{table_id}"
    
    job_config = google.cloud.bigquery.LoadJobConfig(
        source_format=google.cloud.bigquery.SourceFormat.PARQUET,
        write_disposition=google.cloud.bigquery.WriteDisposition.WRITE_TRUNCATE,
        autodetect=schema is None,
        schema=schema,
    )
    
    job = bq_client.load_table_from_uri(
        gcs_uri,
        table_ref,
        job_config=job_config,
    )
    try:
        job.result()
        table = bq_client.get_table(table_ref)
        logger.info("bq_load_complete",
            table_id=table_id,
            row_count=table.num_rows
        )
    except google.cloud.exceptions.NotFound as e:
        logger.error("bq_table_not_found", error=str(e), table_ref=table_ref)
        raise
    except google.cloud.exceptions.BadRequest as e:
        logger.error("bq_bad_request", error=str(e), table_ref=table_ref, gcs_uri=gcs_uri)
        raise
    except google.cloud.exceptions.GoogleCloudError as e:
        logger.error("bq_load_failed", error=str(e), table_ref=table_ref, gcs_uri=gcs_uri)
        raise

if __name__ == "__main__":
    from google.cloud import storage
    from ingest.watermark import read_watermark

    gcs_client = storage.Client()
    bq_client = google.cloud.bigquery.Client()
    current = read_watermark(gcs_client)
    

    load_to_bigquery(bq_client, 
    gcs_uri=f"gs://{GCS_BUCKET}/silver/Patient/{current}/patient.parquet",
    table_id="silver_patient",
    project=GCP_PROJECT_ID,
    dataset=BQ_DATASET
    )
    
    load_to_bigquery(bq_client, 
        gcs_uri=f"gs://{GCS_BUCKET}/silver/ExplanationOfBenefit/{current}/eob_part_*.parquet",
        table_id="silver_eob",
        project=GCP_PROJECT_ID,
        dataset=BQ_DATASET
    )

    load_to_bigquery(bq_client, 
        gcs_uri=f"gs://{GCS_BUCKET}/silver/Coverage/{current}/coverage.parquet",
        table_id="silver_coverage",
        project=GCP_PROJECT_ID,
        dataset=BQ_DATASET,
        schema=coverage_schema
    )