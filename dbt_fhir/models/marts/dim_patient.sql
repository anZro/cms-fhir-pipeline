with patients as (
    select 
        patient_id,
        birth_date,
        gender,
        deceased,
        state,
        postal_code,
        race_code,
        race_display,
        mbi
    from {{ ref('stg_patient') }}
),

coverages as (
    select
        patient_id,
        medicare_part,
        ms_cd_code,
        ms_cd_display,
        termination_code,
        reference_year,
        row_number() over(partition by patient_id order by last_updated desc) as rn
    from {{ ref('stg_coverage') }}
    where medicare_part = 'Part A'
    
),

ranked_cvg as (
    select
        patient_id,
        medicare_part,
        ms_cd_code,
        ms_cd_display,
        termination_code,
        reference_year
    from coverages
    where rn = 1
)

select
    p.patient_id,
    p.birth_date,
    p.gender,
    p.deceased,
    p.state,
    p.postal_code,
    p.race_code,
    p.race_display,
    p.mbi,
    c.medicare_part,
    c.ms_cd_code,
    c.ms_cd_display,
    c.termination_code,
    c.reference_year
from patients p
left join ranked_cvg c using(patient_id)