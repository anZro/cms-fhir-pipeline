import os
import structlog
from google.cloud import storage
from google.cloud.exceptions import NotFound
from dotenv import load_dotenv

logger = structlog.get_logger(__name__)
load_dotenv()

GCS_BUCKET = os.getenv("GCS_BUCKET")
WATERMARK_PATH = "bronze/watermark.txt"

def read_watermark(gcs_client, bucket_name:str = GCS_BUCKET) -> str | None:
    bucket = gcs_client.bucket(bucket_name)
    blob = bucket.blob(WATERMARK_PATH)

    try:
        contents = blob.download_as_text()
        if not contents or contents.strip() == "":
            logger.warning("watermark_empty")
            return None
        logger.info("watermark_read", 
            contents=contents.strip(),
            )
        return contents.strip()
    except NotFound:
        logger.info("watermark_not_found")
        return None

def write_watermark(gcs_client, bucket_name:str = GCS_BUCKET, transaction_time:str = None) -> None:
    if not transaction_time:
        logger.warning("watermark_write_skipped",
        reason="transaction_time is None")
        return

    bucket = gcs_client.bucket(bucket_name)
    blob = bucket.blob(WATERMARK_PATH)
    blob.upload_from_string(transaction_time)
    logger.info("watermark_written",
        transaction_time=transaction_time,
        )

if __name__ == "__main__": 
    gcs_client = storage.Client()
    
    # read current watermark (None on first run)
    current = read_watermark(gcs_client)
    logger.info("current_watermark", value=current)

    # hardcoded test — confirm GCS read/write works before running full export
    test_timestamp = "2025-11-12T12:10:07.068Z"
    write_watermark(gcs_client, transaction_time=test_timestamp)

    ''' 
    from ingest.auth import get_token
    from ingest.export import start_export, poll_job
      
    # get a real transaction_time from BCDA
    token = get_token()
    job_url = start_export(token, since=current)
    result = poll_job(token, job_url)
   
    # write watermark to GCS
    write_watermark(gcs_client, transaction_time=result.transaction_time)
    '''
    
    # confirm watermark is persisted
    confirmed = read_watermark(gcs_client)
    logger.info("confirmed_watermark", value=confirmed)