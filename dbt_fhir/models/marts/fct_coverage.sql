with eob_summary as (
    select
        patient_id,
        MIN(service_start_date) as first_service_date,
        MAX(service_start_date) as last_service_date,
        COUNT(eob_id) as claim_count
    from {{ ref('stg_eob') }}
    group by patient_id
)

select
    c.patient_id,
    c.medicare_part,
    c.status,
    c.ms_cd_code,
    c.ms_cd_display,
    c.termination_code,
    c.reference_year,
    c.dual_eligible,
    e.first_service_date,
    e.last_service_date,
    e.claim_count
from {{ ref('stg_coverage') }} c
left join eob_summary e using(patient_id)