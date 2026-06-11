with source as (
    select * from {{ source('silver', 'silver_patient') }}
),

casted as (
    select
        patient_id,
        cast(birth_date as date) as birth_date,
        gender,
        deceased,
        state,
        postal_code,
        race_code,
        race_display,
        mbi,
        cast(last_updated as timestamp) as last_updated,
        CURRENT_TIMESTAMP() as loaded_at
    from source
)

select *
from casted