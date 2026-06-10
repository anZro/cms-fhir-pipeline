import json
import io
import pandas as pd
import structlog
import os
from dotenv import load_dotenv

load_dotenv()

logger = structlog.get_logger(__name__)

GCS_BUCKET = os.getenv("GCS_BUCKET")

def flatten_eob(record: dict) -> dict:
    # direct .get() — root level
    eob_id = record.get("id")
    patient_id = record.get("patient", {}).get("reference", "").replace("Patient/", "")

    claim_type = next(
        (coding["code"]
            for coding in record.get("type", {}).get("coding", [])
            if "eob-type" in coding.get("system", "")
            ), None       
        )

    claim_subtype = record.get("subType", {}).get("coding", [{}])[0].get("code")
    service_start_date = record.get("billablePeriod", {}).get("start")
    service_end_date = record.get("billablePeriod", {}).get("end")

    provider_npi = next(
        (entry["provider"]["identifier"]["value"]
            for entry in record.get("careTeam", [])
            if entry.get("role", {}).get("coding", [{}])[0].get("code") == "attending"
            and entry.get("provider", {}).get("identifier", {}).get("type", {}).get("coding", [{}])[0].get("code") == "npi"), None)
    
    facility_state = record.get("item", [{}])[0].get("locationAddress", {}).get("state")
    total_payment = record.get("payment", {}).get("amount", {}).get("value", 0)
    total_amount = record.get("total", [{}])[0].get("amount", {}).get("value", 0)

    primary_diagnosis_code = next(
        (entry["diagnosisCodeableConcept"]["coding"][0].get("code")
            for entry in record.get("diagnosis", [])
            if entry.get("type", [{}])[0].get("coding", [{}])[0].get("code") == "principal"),
        None)

    primary_diagnosis_display = next(
        (entry["diagnosisCodeableConcept"]["coding"][0].get("display")
            for entry in record.get("diagnosis", [])
            if entry.get("type", [{}])[0].get("coding", [{}])[0].get("code") == "principal"),
        None)

    admission_type = next(
        (entry["code"]["coding"][0]["display"]
            for entry in record.get("supportingInfo", [])
            if entry.get("category", {}).get("coding", [{}])[0].get("code") == "admtype"), None)

    discharge_status = next(
        (entry["code"]["coding"][0]["code"]
            for entry in record.get("supportingInfo", [])
            if entry.get("category", {}).get("coding", [{}])[0].get("code") == "discharge-status"), None)

    diagnosis_json = json.dumps(record.get("diagnosis", []))
    last_updated = record.get("meta", {}).get("lastUpdated")

    return {
        "eob_id": eob_id,
        "patient_id": patient_id,
        "service_start_date": service_start_date,
        "service_end_date": service_end_date,
        "provider_npi": provider_npi,
        "facility_state": facility_state,
        "total_payment": total_payment,
        "total_amount": total_amount,
        "primary_diagnosis_code": primary_diagnosis_code,
        "primary_diagnosis_display": primary_diagnosis_display,
        "diagnosis_json": diagnosis_json,
        "last_updated": last_updated,
        "claim_type": claim_type,
        "claim_subtype": claim_subtype,
        "admission_type": admission_type,
        "discharge_status": discharge_status,
    }


def transform_eob(gcs_client, transaction_time: str, bucket_name:str = GCS_BUCKET):
    bucket = gcs_client.bucket(bucket_name)
    blobs = bucket.list_blobs(
        prefix=f"bronze/ExplanationOfBenefit/{transaction_time}"
    )

    rows = []
    for blob in blobs:
        if blob.name.endswith(".ndjson"):
            with blob.open("rt") as f:
                for line in f:
                    record = json.loads(line)
                    flattened = flatten_eob(record)
                    rows.append(flattened)
    
    
    result_df = pd.DataFrame(rows)
    
    #convert dataframe to parquet bytes in memory
    buffer = io.BytesIO()
    result_df.to_parquet(buffer, index=False, engine="pyarrow")
    buffer.seek(0)
    
    #upload to GCS
    blob = bucket.blob(f"silver/ExplanationOfBenefit/{transaction_time}/eob.parquet")
    blob.upload_from_file(buffer, content_type="application/octet-stream")
    logger.info("eob_transform_complete",
                bucket_name=bucket_name,
                row_count=len(result_df),
                file_path=f"silver/ExplanationOfBenefit/{transaction_time}/eob.parquet")
    
if __name__ == "__main__":
    from google.cloud import storage
    from ingest.watermark import read_watermark
    
    gcs_client = storage.Client()
    
    #read current watermark from GCS
    current = read_watermark(gcs_client)
    logger.info("current_watermark", value=current)

    #transform Patient data
    transform_eob(gcs_client, transaction_time=current)
    logger.info("transform_complete", 
                transaction_time=current)