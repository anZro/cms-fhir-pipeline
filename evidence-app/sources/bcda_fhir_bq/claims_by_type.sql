select
    claim_type,
    count(*) as claim_count,
    sum(total_payment) as total_payment,
    avg(total_payment) as avg_payment
from `cms-fhir-pipeline.bcda_fhir.fct_claims`
where claim_type is not null
group by claim_type
order by claim_count desc