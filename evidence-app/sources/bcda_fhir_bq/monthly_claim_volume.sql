select
    date_trunc(service_start_date, month) as claim_month,
    count(*) as claim_count
from `cms-fhir-pipeline.bcda_fhir.fct_claims`
where service_start_date is not null
and service_start_date >= '2015-01-01'
group by claim_month
order by claim_month