-- population_overview.sql
select
    count(*) as total_beneficiaries,
    countif(gender = 'female') as female_count,
    countif(gender = 'male') as male_count
from `cms-fhir-pipeline.bcda_fhir.dim_patient`