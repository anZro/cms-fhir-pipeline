with source as (
    select * from {{ source('silver', 'silver_eob') }}
),

casted as (
    select
        eob_id,
        patient_id,
        cast(service_start_date as date) as service_start_date,
        cast(service_end_date as date) as service_end_date,
        provider_npi,
        facility_state,
        cast(total_payment as numeric) as total_payment,
        cast(total_amount as numeric) as total_amount,
        primary_diagnosis_code,
        primary_diagnosis_display,
        diagnosis_json,
        cast(last_updated as timestamp) as last_updated,
        claim_type,
        claim_subtype,
        admission_type,
        discharge_status,
        CURRENT_TIMESTAMP() as loaded_at
    from source
)

select *
from casted