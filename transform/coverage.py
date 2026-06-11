import json
import io
import pandas as pd
import structlog
import os
from dotenv import load_dotenv

load_dotenv()

logger = structlog.get_logger(__name__)

GCS_BUCKET = os.getenv("GCS_BUCKET")

def flatten_coverage(record: dict) -> dict:
    # direct .get() — root level
    coverage_id     = record.get("id")
    status          = record.get("status")
    subscriber_id   = record.get("subscriberId")

    # nested — strip "Patient/" prefix
    patient_id      = record.get("beneficiary", {}).get("reference", "").replace("Patient/", "")

    # class array — filter where type.coding[0].code == "plan" → value
    medicare_part   = next((c["value"]
                        for c in record.get("class", [])
                        if c.get("type", {}).get("coding", [{}])[0].get("code") == "plan"), None)

    #schema reserved values
    period_start    = record.get("period", {}).get("start")
    period_end      = record.get("period", {}).get("end")
    dual_eligible   = next((ext["valueCoding"]["code"]
                        for ext in record.get("extension", [])
                        if "dual_eligible" in ext.get("url", "")), None)
    
    # extension filters
    ms_cd_code      = next((ext["valueCoding"]["code"]
                        for ext in record.get("extension", [])
                        if "ms_cd" in ext.get("url", "")), None)

    ms_cd_display   = next((ext["valueCoding"]["display"]
                        for ext in record.get("extension", [])
                        if "ms_cd" in ext.get("url", "")), None)

    termination_code = next((ext["valueCoding"]["code"]
                        for ext in record.get("extension", [])
                        if "a_trm_cd" in ext.get("url", "")), None)

    reference_year  = next((ext["valueDate"]
                        for ext in record.get("extension", [])
                        if "rfrnc_yr" in ext.get("url", "")), None)

    # nested dict
    last_updated    = record.get("meta", {}).get("lastUpdated")

    return {
        "coverage_id": coverage_id,
        "status": status,
        "subscriber_id": subscriber_id,
        "patient_id": patient_id,
        "medicare_part": medicare_part,
        "period_start": period_start,
        "period_end": period_end,
        "dual_eligible": dual_eligible,
        "ms_cd_code": ms_cd_code,
        "ms_cd_display": ms_cd_display,
        "termination_code": termination_code,
        "reference_year": reference_year,
        "last_updated": last_updated,
    }


def transform_coverage(gcs_client, transaction_time: str, bucket_name:str = GCS_BUCKET):
    bucket = gcs_client.bucket(bucket_name)
    blobs = bucket.list_blobs(
        prefix=f"bronze/Coverage/{transaction_time}"
    )

    rows = []
    for blob in blobs:
        if blob.name.endswith(".ndjson"):
            with blob.open("rt") as f:
                for line in f:
                    record = json.loads(line)
                    flattened = flatten_coverage(record)
                    rows.append(flattened)
    
    
    result_df = pd.DataFrame(rows)
    # explicit typing for sparse columns — BCDA sandbox doesn't populate period dates
    # pyarrow infers INT64 for all-null columns without explicit casting
    result_df["period_start"] = result_df["period_start"].astype(str).where(result_df["period_start"].notna(), other=None)
    result_df["period_end"] = result_df["period_end"].astype(str).where(result_df["period_end"].notna(), other=None)
    result_df["dual_eligible"] = result_df["dual_eligible"].astype(str).where(result_df["dual_eligible"].notna(), other=None)
    
    #convert dataframe to parquet bytes in memory
    buffer = io.BytesIO()
    result_df.to_parquet(buffer, index=False, engine="pyarrow")
    buffer.seek(0)
    
    #upload to GCS
    blob = bucket.blob(f"silver/Coverage/{transaction_time}/coverage.parquet")
    blob.upload_from_file(buffer, content_type="application/octet-stream")
    logger.info("coverage_transform_complete",
                bucket_name=bucket_name,
                row_count=len(result_df),
                file_path=f"silver/Coverage/{transaction_time}/coverage.parquet")
    
if __name__ == "__main__":
    from google.cloud import storage
    from ingest.watermark import read_watermark
    
    gcs_client = storage.Client()
    
    #read current watermark from GCS
    current = read_watermark(gcs_client)
    logger.info("current_watermark", value=current)

    #transform Patient data
    transform_coverage(gcs_client, transaction_time=current)
    logger.info("transform_complete", 
                transaction_time=current)