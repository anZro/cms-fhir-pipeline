select
    count(*) as total_claims,
    sum(total_payment) as total_payments,
    avg(total_payment) as avg_payment_per_claim
from `cms-fhir-pipeline.bcda_fhir.fct_claims`