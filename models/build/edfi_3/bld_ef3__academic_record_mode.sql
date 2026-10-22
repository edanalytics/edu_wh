with stg_academic_record as (
    select * from {{ ref('stg_ef3__student_academic_records') }}
),
seed_academic_record_mode as (
    select * from {{ ref('xwalk_tenant_academic_record_mode') }}
),
longitudinal_mode as (
    select stg_academic_record.*,
    -- keeping this column here temporarily for qc
    seed_academic_record_mode.academic_record_mode
    from stg_academic_record
    left join seed_academic_record_mode
        on stg_academic_record.tenant_code = seed_academic_record_mode.tenant_code
    where (academic_record_mode = 'longitudinal'
        or academic_record_mode is null)
    qualify api_year = max(api_year) over(partition by tenant_code, ed_org_id, student_unique_id)
),
single_year_mode as (
    select stg_academic_record.*,
    -- keeping this column here temporarily for qc
    seed_academic_record_mode.academic_record_mode
    from stg_academic_record
    join seed_academic_record_mode
        on stg_academic_record.tenant_code = seed_academic_record_mode.tenant_code
    where academic_record_mode = 'single_year'
    and stg_academic_record.api_year = stg_academic_record.school_year
)
select * from longitudinal_mode
union all
select * from single_year_mode
