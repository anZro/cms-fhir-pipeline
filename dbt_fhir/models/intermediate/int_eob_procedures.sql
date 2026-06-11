-- Procedure codes (item[*].productOrService) are not populated in the
-- BCDA sandbox dataset — productOrService.coding[0].code = "NULL" across
-- all records. This model is scaffolded for production use where real
-- CPT/HCPCS codes would be present.
-- 
-- To enable: add item_json = json.dumps(record.get("item", [])) to
-- transform/eob.py and re-run the transform + load pipeline.

select null as eob_id, null as procedure_code
from {{ ref('stg_eob') }}
where false