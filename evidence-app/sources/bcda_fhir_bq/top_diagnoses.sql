select
    diagnosis_code,
    diagnosis_display,
    count(*) as diagnosis_count
from `cms-fhir-pipeline.bcda_fhir.int_eob_diagnoses`
where diagnosis_code is not null
group by diagnosis_code, diagnosis_display
order by diagnosis_count desc
limit 20