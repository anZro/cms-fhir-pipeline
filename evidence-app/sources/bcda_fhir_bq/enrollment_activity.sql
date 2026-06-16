select
    date_trunc(first_service_date, year) as enrollment_year,
    count(distinct patient_id) as patient_count
from `cms-fhir-pipeline.bcda_fhir.fct_coverage`
where first_service_date is not null
group by enrollment_year
order by enrollment_year