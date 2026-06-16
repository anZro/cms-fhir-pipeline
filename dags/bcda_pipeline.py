# Import Functions
from ingest.auth import get_token
from ingest.export import start_export, poll_job
from ingest.watermark import read_watermark, write_watermark
from ingest.download import download_to_gcs
from ingest.load_bq import load_to_bigquery
from transform.patient import transform_patient
from transform.coverage import transform_coverage
from transform.eob import transform_eob

# Import Standard Libraries
from airflow.decorators import dag, task, task_group
from datetime import datetime, timedelta
from google.cloud import storage, bigquery
import structlog
import os

from dotenv import load_dotenv
load_dotenv("/usr/local/airflow/.env")

logger = structlog.get_logger(__name__)

def failure_alert(context):
    import requests
    task_id = context["task_instance"].task_id
    dag_id = context["task_instance"].dag_id
    log_url = context["task_instance"].log_url
    
    slack_webhook = os.getenv("SLACK_WEBHOOK_URL")
    if slack_webhook:
        requests.post(
            slack_webhook,
            json={
                "text": f"❌ *{dag_id}* — task `{task_id}` failed\n<{log_url}|View logs>"
            }
        )

def success_alert(context):
    import requests
    dag_id = context["dag_run"].dag_id
    run_id = context["dag_run"].run_id
    start_date = context["dag_run"].start_date
    end_date = context["dag_run"].end_date
    
    if start_date and end_date:
        duration = end_date - start_date
        minutes, seconds = divmod(duration.seconds, 60)
        duration_str = f"{minutes}m {seconds}s"
    else:
        duration_str = "unknown"
    
    slack_webhook = os.getenv("SLACK_WEBHOOK_URL")
    if slack_webhook:
        requests.post(
            slack_webhook,
            json={
                "text": f"✅ *{dag_id}* completed successfully\n⏱ Duration: {duration_str}\nRun ID: `{run_id}`"
            }
        )

@dag(
    schedule="@weekly",
    start_date=datetime(2025, 1, 1),
    catchup=False,
    on_success_callback=success_alert,
    default_args={
        "retries": 3,
        "retry_delay": timedelta(minutes=2),
        "on_failure_callback": failure_alert,
        "execution_timeout": timedelta(hours=2),
    },
    tags=["bcda", "fhir", "cms"]
)
def bcda_pipeline():

    @task
    def get_token_task() -> str:
        return get_token()

    @task
    def read_watermark_task() -> str:
        gcs_client = storage.Client()
        return read_watermark(gcs_client)

    @task
    def start_export_task(token:str, since:str | None) -> str:
        return start_export(token, since=since)

    @task
    def poll_export_task(token: str, job_url: str) -> dict:
        result = poll_job(token, job_url,
            client_id=os.getenv('BCDA_CLIENT_ID'),
            client_secret=os.getenv('BCDA_CLIENT_SECRET'),
        )
        return {
            'urls': result.urls, 
            'transaction_time': result.transaction_time
        }

    @task_group
    def download_group(token, urls, transaction_time):
        @task
        def download_patient(token, urls, transaction_time):
            gcs_client = storage.Client()
            bucket_name = os.getenv("GCS_BUCKET")
            bucket = gcs_client.bucket(bucket_name)

            # clear any existing files under this prefix before downloading fresh —
            # BCDA can return the same transaction_time across runs (e.g. static
            # sandbox data), and UUID-named files would otherwise accumulate
            # instead of overwriting
            prefix = f"bronze/Patient/{transaction_time}/"
            existing = list(bucket.list_blobs(prefix=prefix))
            for blob in existing:
                blob.delete()
            if existing:
                print(f"Cleared {len(existing)} existing files under {prefix}")

            token = get_token(
                client_id=os.getenv("BCDA_CLIENT_ID"),
                client_secret=os.getenv("BCDA_CLIENT_SECRET")
            )
            for item in urls:
                if item["type"] == "Patient":
                    token = get_token(
                        client_id=os.getenv("BCDA_CLIENT_ID"),
                        client_secret=os.getenv("BCDA_CLIENT_SECRET")
                    )
                    download_to_gcs(
                        token=token,
                        url=item["url"],
                        resource_type=item["type"],
                        transaction_time=transaction_time,
                        gcs_client=gcs_client,
                        bucket_name=bucket_name
                    )

        @task
        def download_coverage(token, urls, transaction_time):
            gcs_client = storage.Client()
            bucket_name = os.getenv("GCS_BUCKET")
            bucket = gcs_client.bucket(bucket_name)

            # clear any existing files under this prefix before downloading fresh —
            # BCDA can return the same transaction_time across runs (e.g. static
            # sandbox data), and UUID-named files would otherwise accumulate
            # instead of overwriting
            prefix = f"bronze/Coverage/{transaction_time}/"
            existing = list(bucket.list_blobs(prefix=prefix))
            for blob in existing:
                blob.delete()
            if existing:
                print(f"Cleared {len(existing)} existing files under {prefix}")

            token = get_token(
                client_id=os.getenv("BCDA_CLIENT_ID"),
                client_secret=os.getenv("BCDA_CLIENT_SECRET")
            )
            for item in urls:
                if item["type"] == "Coverage":
                    token = get_token(
                        client_id=os.getenv("BCDA_CLIENT_ID"),
                        client_secret=os.getenv("BCDA_CLIENT_SECRET")
                    )
                    download_to_gcs(
                        token=token,
                        url=item["url"],
                        resource_type=item["type"],
                        transaction_time=transaction_time,
                        gcs_client=gcs_client,
                        bucket_name=bucket_name
                    )

        @task
        def download_eob(token, urls, transaction_time):
            gcs_client = storage.Client()
            bucket_name = os.getenv("GCS_BUCKET")
            bucket = gcs_client.bucket(bucket_name)

            # clear any existing files under this prefix before downloading fresh —
            # BCDA can return the same transaction_time across runs (e.g. static
            # sandbox data), and UUID-named files would otherwise accumulate
            # instead of overwriting
            prefix = f"bronze/ExplanationOfBenefit/{transaction_time}/"
            existing = list(bucket.list_blobs(prefix=prefix))
            for blob in existing:
                blob.delete()
            if existing:
                print(f"Cleared {len(existing)} existing files under {prefix}")

            token = get_token(
                client_id=os.getenv("BCDA_CLIENT_ID"),
                client_secret=os.getenv("BCDA_CLIENT_SECRET")
            )
            for item in urls:
                if item["type"] == "ExplanationOfBenefit":
                    token = get_token(
                        client_id=os.getenv("BCDA_CLIENT_ID"),
                        client_secret=os.getenv("BCDA_CLIENT_SECRET")
                    )
                    download_to_gcs(
                        token=token,
                        url=item["url"],
                        resource_type=item["type"],
                        transaction_time=transaction_time,
                        gcs_client=gcs_client,
                        bucket_name=bucket_name
                    )

        download_patient(token, urls, transaction_time)
        download_coverage(token, urls, transaction_time)
        download_eob(token, urls, transaction_time)

    @task
    def transform_task(transaction_time: str):
        from google.cloud import storage as gcs
        gcs_client = gcs.Client()
        
        # check if any Bronze files exist for this transaction_time
        bucket = gcs_client.bucket(os.getenv("GCS_BUCKET"))
        blobs = list(bucket.list_blobs(prefix=f"bronze/Patient/{transaction_time}"))
        
        if not blobs:
            logger.info("transform_skipped", reason="no bronze files", transaction_time=transaction_time)
            return
        
        transform_patient(gcs_client, transaction_time)
        transform_coverage(gcs_client, transaction_time)
        transform_eob(gcs_client, transaction_time)

    @task
    def load_bq_task(transaction_time: str):
        from ingest.load_bq import load_to_bigquery, coverage_schema
        bq_client = bigquery.Client()
        GCS_BUCKET = os.getenv("GCS_BUCKET")

        load_to_bigquery(bq_client, 
        gcs_uri=f"gs://{GCS_BUCKET}/silver/Patient/{transaction_time}/patient.parquet",
        table_id="silver_patient",
        project=os.getenv("GCP_PROJECT_ID"),
        dataset=os.getenv("BQ_DATASET")
        )
    
        load_to_bigquery(bq_client, 
            gcs_uri=f"gs://{GCS_BUCKET}/silver/ExplanationOfBenefit/{transaction_time}/eob_part_*.parquet",
            table_id="silver_eob",
            project=os.getenv("GCP_PROJECT_ID"),
            dataset=os.getenv("BQ_DATASET")
        )

        load_to_bigquery(bq_client, 
            gcs_uri=f"gs://{GCS_BUCKET}/silver/Coverage/{transaction_time}/coverage.parquet",
            table_id="silver_coverage",
            project=os.getenv("GCP_PROJECT_ID"),
            dataset=os.getenv("BQ_DATASET"),
            schema=coverage_schema
        )

    @task
    def run_dbt_task():
        import subprocess
        subprocess.run(
            [
                "dbt", "run",
                "--project-dir", "/usr/local/airflow/dbt_fhir",
                "--profiles-dir", "/usr/local/airflow/dbt_fhir"
            ],
            check=True
        )

    @task
    def write_watermark_task(transaction_time: str):
        gcs_client = storage.Client()
        write_watermark(gcs_client, transaction_time=transaction_time)

    # wire tasks together
    token = get_token_task()
    since = read_watermark_task()
    job_url = start_export_task(token, since)
    result = poll_export_task(token, job_url)

    downloads = download_group(
        token,
        result["urls"],
        result["transaction_time"]
    )

    transforms = transform_task(result["transaction_time"])
    loads = load_bq_task(result["transaction_time"])
    dbt = run_dbt_task()
    watermark = write_watermark_task(result["transaction_time"])

    downloads >> transforms >> loads >> dbt >> watermark

bcda_pipeline()

    
        
        