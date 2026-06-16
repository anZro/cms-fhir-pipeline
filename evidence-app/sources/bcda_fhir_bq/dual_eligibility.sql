select
    coalesce(dual_eligible, 'Unknown') as dual_eligible_status,
    count(distinct patient_id) as patient_count
from `cms-fhir-pipeline.bcda_fhir.fct_coverage`
group by dual_eligible_status