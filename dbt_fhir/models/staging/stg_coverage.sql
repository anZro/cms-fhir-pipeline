with source as (
    select * from {{ source('silver', 'silver_coverage') }}
),

casted as (
    select
        coverage_id,
        status,
        subscriber_id,
        patient_id,
        medicare_part,
        cast(period_start as date) as period_start,
        cast(period_end as date) as period_end,
        dual_eligible,
        ms_cd_code,
        ms_cd_display,
        termination_code,
        reference_year,
        cast(last_updated as timestamp) as last_updated,
        CURRENT_TIMESTAMP() as loaded_at
    from source
)

select * from casted