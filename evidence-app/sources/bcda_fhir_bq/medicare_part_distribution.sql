select
    medicare_part,
    count(*) as enrollment_count
from `cms-fhir-pipeline.bcda_fhir.fct_coverage`
where medicare_part is not null
group by medicare_part
order by enrollment_count desc