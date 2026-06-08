from typing import Optional
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
import requests
import structlog
import os
from dotenv import load_dotenv

load_dotenv()

BCDA_BASE_URL = os.getenv("BCDA_BASE_URL")
BCDA_CLIENT_ID = os.getenv("BCDA_CLIENT_ID")
BCDA_CLIENT_SECRET = os.getenv("BCDA_CLIENT_SECRET")

logger = structlog.get_logger(__name__)

@dataclass
class BCDATokenCache:
    token: Optional[str] = None
    fetched_at: Optional[datetime] = None

    @property
    def is_valid(self) -> bool:
        if not self.token or not self.fetched_at:
            return False
        return datetime.now(timezone.utc) - self.fetched_at < timedelta(minutes=19)

# Cache for BCDA client
_cache = BCDATokenCache()

def get_token(base_url: str = BCDA_BASE_URL, client_id: str = BCDA_CLIENT_ID, client_secret: str = BCDA_CLIENT_SECRET) -> str:
    global _cache
    if _cache.is_valid:
        return _cache.token

    #fetch new token
    url = f"{base_url}/auth/token"
    try:
        resp = requests.post(url, 
            auth=(client_id, client_secret), 
            headers={"Accept": "application/json"},
            timeout=10,
        )
        resp.raise_for_status()
        data = resp.json()
        _cache.token = data["access_token"]
        _cache.fetched_at = datetime.now(timezone.utc)
        return _cache.token
    
    except requests.exceptions.HTTPError as e:
        logger.error("token_fetch_failed",
        error=str(e),
        url=url,
        status_code=e.response.status_code if e.response is not None else None,
        )
        raise
    except requests.exceptions.ConnectionError as e:
        logger.error("token_connection_failed",
        error=str(e),
        url=url,
        )
        raise
    except requests.exceptions.Timeout as e:
        logger.error("token_timeout",
        error=str(e),
        url=url,
        timeout=10
        )
        raise
    except requests.exceptions.RequestException as e:
        logger.error("token_request_failed",
        error=str(e),
        url=url
        )
        raise

if __name__ == "__main__":    
    token = get_token()
    print(f"Token received: {token[:20]}...")