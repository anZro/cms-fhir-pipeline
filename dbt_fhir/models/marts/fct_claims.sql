select
    e.eob_id,
    e.patient_id,
    e.claim_type, 
    e.claim_subtype,
    e.service_start_date,
    e.service_end_date,
    e.primary_diagnosis_code,
    e.primary_diagnosis_display,
    e.provider_npi,
    e.facility_state,
    e.total_payment,
    e.total_amount,
    e.admission_type,
    e.discharge_status,
    p.gender,
    p.state,
    p.race_code,
    p.reference_year
from {{ ref('stg_eob') }} e
left join {{ ref('dim_patient') }} p using(patient_id)