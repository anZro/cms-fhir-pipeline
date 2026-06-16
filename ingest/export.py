import requests
from typing import NamedTuple
import structlog
import os
import time
from dotenv import load_dotenv
from ingest.auth import get_token

load_dotenv()

logger = structlog.get_logger(__name__)

BCDA_BASE_URL = os.getenv("BCDA_BASE_URL")

class ExportResult(NamedTuple):
    urls: list[dict]
    transaction_time: str

def start_export(token, base_url: str = BCDA_BASE_URL, since=None) -> str:
    url = f"{base_url}/api/v2/Group/all/$export"

    headers = {
        "Authorization": f"Bearer {token}",
        "Prefer": "respond-async",
        "Accept": "application/fhir+json"
    }

    params = {}
    if since:
        params["_since"] = since

    logger.info("export_job_starting",
        url=url,
        incremental=since is not None,
        since=since
    )

    try:
        response = requests.get(url, headers=headers, params=params, timeout=30)
        
        if response.status_code != 202:
            response.raise_for_status()

        job_url = response.headers["Content-Location"]
        logger.info("export_job_started", job_url=job_url)
        return job_url
    except requests.exceptions.HTTPError as e:
        logger.error("export_job_failed", 
            url=url,
            status_code=e.response.status_code if e.response is not None else None,
            )
        raise
    except requests.exceptions.ConnectionError as e:
        logger.error("export_connection_failed",
        error=str(e),
        url=url,
        )
        raise
    except requests.exceptions.Timeout as e:
        logger.error("export_timeout",
        error=str(e),
        url=url,
        timeout=30,
        )
        raise
    except requests.exceptions.RequestException as e:
        logger.error("export_request_failed",
        error=str(e),
        url=url,
        )
        raise

def poll_job(token, job_url, poll_interval=30, client_id=None, client_secret=None) -> ExportResult:
    try:
        poll_count = 0
        while True:
            poll_count += 1

            # refresh token if needed
            if client_id and client_secret:
                token = get_token(client_id=client_id, client_secret=client_secret)
            
            #redefine headers to send the new token
            headers = {
                "Authorization": f"Bearer {token}",
            }
            
            response = requests.get(job_url, headers=headers, timeout=30)
            progress = response.headers.get("X-Progress", "unknown")

            if response.status_code == 202:
                logger.info("export_job_in_progress",
                progress=progress,
                poll_count=poll_count,
                )
                time.sleep(poll_interval)

            elif response.status_code == 200:
                data = response.json()
                resource_types = [item["type"] for item in data.get("output", [])]
                logger.info("export_job_complete",
                    transaction_time=data.get("transactionTime"),
                    resource_types=resource_types,
                    file_count=len(data.get("output", []))
                )
                return ExportResult(
                    urls=data.get("output", []),
                    transaction_time=data.get("transactionTime")
                )

            else:
                response.raise_for_status()
                
    except requests.exceptions.HTTPError as e:
        logger.error("export_job_failed", 
            url=job_url,
            status_code=e.response.status_code if e.response is not None else None,
            )
        raise
    except requests.exceptions.ConnectionError as e:
        logger.error("export_connection_failed",
        error=str(e),
        url=job_url,
        )
        raise
    except requests.exceptions.Timeout as e:
        logger.error("export_timeout",
        error=str(e),
        url=job_url,
        timeout=30,
        )
        raise
    except requests.exceptions.RequestException as e:
        logger.error("export_request_failed",
        error=str(e),
        url=job_url,
        )
        raise   

if __name__ == "__main__":
    from ingest.auth import get_token
    token = get_token()
    job_url = start_export(token)
    result = poll_job(token, job_url)
    logger.info("export_completed",
        file_count = len(result.urls),
        transaction_time=result.transaction_time
    )