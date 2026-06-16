-- gender_distribution.sql
select
    gender,
    count(*) as patient_count
from `cms-fhir-pipeline.bcda_fhir.dim_patient`
group by gender