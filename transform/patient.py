import json
import io
import pandas as pd
import structlog
import os
from dotenv import load_dotenv

load_dotenv()

logger = structlog.get_logger(__name__)

GCS_BUCKET = os.getenv("GCS_BUCKET")

def flatten_patient(record: dict) -> dict:
    #flatten FHIR records and extract necessary fields
    #Root level
    patient_id = record.get("id")
    birth_date = record.get("birthDate")
    gender = record.get("gender")
    deceased = record.get("deceasedBoolean")
    
    #list index 0 - guard against empty lists
    state = record.get("address", [{}])[0].get("state")
    postal_code = record.get("address", [{}])[0].get("postalCode")

    #filter extension list by url
    race_code = next((ext["valueCoding"]["code"] 
                        for ext in record.get("extension", [])
                        if "race" in ext.get("url", "")),
                        None)

    race_display = next((ext["valueCoding"]["display"] 
                        for ext in record.get("extension", [])
                        if "race" in ext.get("url", "")),
                        None)

    #filter identifier list by system
    mbi = next((ext["value"]
            for ext in record.get("identifier", [])
            if "us-mbi" in ext.get("system", "")
            ), None)
    
    # nested dict
    last_updated = record.get("meta", {}).get("lastUpdated")

    return {
        "patient_id": patient_id,
        "birth_date": birth_date,
        "gender": gender,
        "deceased": deceased,
        "state": state,
        "postal_code": postal_code,
        "race_code": race_code,
        "race_display": race_display,
        "mbi": mbi,
        "last_updated": last_updated,
    }


def transform_patient(gcs_client, transaction_time: str, bucket_name:str = GCS_BUCKET):
    bucket = gcs_client.bucket(bucket_name)
    blobs = bucket.list_blobs(
        prefix=f"bronze/Patient/{transaction_time}"
    )
    blobs = [b for b in blobs if b.name.endswith(".ndjson")]

    if not blobs:
        logger.warning("patient_transform_skipped", reason="no records found")
        return

    all_rows = []
    for blob in blobs:
        rows = []
        with blob.open("rt") as f:
                for line in f:
                    record = json.loads(line)
                    flattened = flatten_patient(record)
                    rows.append(flattened)
        all_rows.extend(rows)
        logger.info("patient_blob_processed",
            blob=blob.name,
            row_count=len(rows)
        )
        del rows  # free memory after each blob
    
    if len(all_rows) == 0:
        logger.warning("patient_transform_skipped",
            reason="no records found",
            transaction_time=transaction_time
        )
        return
    
    result_df = pd.DataFrame(all_rows)
    del all_rows
    
    #convert dataframe to parquet bytes in memory
    buffer = io.BytesIO()
    result_df.to_parquet(buffer, index=False, engine="pyarrow")
    buffer.seek(0)
    
    #upload to GCS
    blob = bucket.blob(f"silver/Patient/{transaction_time}/patient.parquet")
    blob.upload_from_file(buffer, content_type="application/octet-stream")
    logger.info("patient_transform_complete",
                bucket_name=bucket_name,
                row_count=len(result_df),
                file_path=f"silver/Patient/{transaction_time}/patient.parquet")
    
if __name__ == "__main__":
    from google.cloud import storage
    from ingest.watermark import read_watermark
    
    gcs_client = storage.Client()
    
    #read current watermark from GCS
    current = read_watermark(gcs_client)
    logger.info("current_watermark", value=current)

    #transform Patient data
    transform_patient(gcs_client, transaction_time=current)
    logger.info("transform_complete", 
                transaction_time=current)