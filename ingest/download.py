import os
import requests
import structlog
from dotenv import load_dotenv
from google.cloud import storage

load_dotenv()

logger = structlog.get_logger(__name__)

GCS_BUCKET = os.getenv("GCS_BUCKET")

def download_to_gcs(token, url: str, resource_type: str, transaction_time: str, gcs_client, bucket_name:str = GCS_BUCKET):
    filename = url.split("/")[-1].replace(".ndjson", "")
    gcs_path = f"bronze/{resource_type}/{transaction_time}/{filename}.ndjson"
    bucket = gcs_client.bucket(bucket_name)
    blob = bucket.blob(gcs_path)

    headers = {
        "Authorization": f"Bearer {token}",
        "Accept-Encoding": "gzip",
    }
    try:
        logger.info("file_download_started",
                resource_type=resource_type,
                url=url
                )
        with requests.get(url, headers=headers, stream=True, timeout=300) as resp:
            resp.raise_for_status()
            with blob.open("wb") as f:
                for chunk in resp.iter_content(chunk_size=8192):
                    if chunk:
                        f.write(chunk)
        logger.info("file_download_complete",
            resource_type=resource_type,
            gcs_path=gcs_path
            )
        return gcs_path
    except requests.exceptions.HTTPError as e:
        logger.error("download_http_error",
        resource_type=resource_type,
        url=url,
        status_code=e.response.status_code if e.response is not None else None,
        )
        raise
    except requests.exceptions.ConnectionError as e:
        logger.error("download_connection_error",
        resource_type=resource_type,
        url=url,
        )
        raise
    except requests.exceptions.Timeout as e:
        logger.error("download_timeout",
        resource_type=resource_type,
        url=url,
        timeout=10,
        )
        raise
    except requests.exceptions.RequestException as e:
        logger.error("download_request_error",
        resource_type=resource_type,
        url=url,
        )
        raise

if __name__ == "__main__":
    from ingest.auth import get_token
    from ingest.export import start_export, poll_job
    from ingest.watermark import read_watermark, write_watermark
    
    gcs_client = storage.Client()

    #read current watermark
    current = read_watermark(gcs_client)
    logger.info("current_watermark", value=current)

    #get a real watermark from BCDA
    token = get_token()
    job_url = start_export(token, since=current)
    result = poll_job(token, job_url,
        client_id=os.getenv("BCDA_CLIENT_ID"),
        client_secret=os.getenv("BCDA_CLIENT_SECRET")
    )

    #write new watermark to GCS
    write_watermark(gcs_client, transaction_time=result.transaction_time)

    #for each url in the export result, download the file to GCS
    files = [download_to_gcs(token, item["url"], item["type"], result.transaction_time, gcs_client) for item in result.urls]
    logger.info("files_downloaded", gcs_path=files, file_count=len(files))