with diagnoses as (
    select
        eob_id,
        patient_id,
        diag
    from {{ ref('stg_eob') }},
    UNNEST(JSON_EXTRACT_ARRAY(diagnosis_json)) as diag
),

extracted as (
    select
        eob_id,
        patient_id,
        JSON_EXTRACT_SCALAR(diag, '$.diagnosisCodeableConcept.coding[0].code') as diagnosis_code,
        JSON_EXTRACT_SCALAR(diag, '$.diagnosisCodeableConcept.coding[0].display') as diagnosis_display,
        JSON_EXTRACT_SCALAR(diag, '$.type[0].coding[0].code') as diagnosis_type
    from diagnoses
)

select * from extracted