select
    race_display,
    count(*) as patient_count
from `cms-fhir-pipeline.bcda_fhir.dim_patient`
where race_display is not null
group by race_display
order by patient_count desc