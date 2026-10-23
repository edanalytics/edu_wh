with stg_academic_record as (
    select * from {{ ref('stg_ef3__student_academic_records') }}
),
detect_academic_record_mode as (
  -- detect whether tenants are pushing only one year of
  -- academic records to each ODS (in which case we need to combine a longitudinal record),
  -- or a longitudinal record to each ODS, in which case we keep the most recent such record
  -- 90% threshold of matching years is arbitrary, but meant to prevent a handful
  -- of mismatched-year academic records from swinging to a longitudinal interpretation
    select
        tenant_code,
        (sum(iff(api_year = school_year, 1, 0)) / count(*)) > 0.9 as is_single_year
    from stg_academic_record
    group by all
),
longitudinal_mode as (
    select stg_academic_record.*
    from stg_academic_record
    left join detect_academic_record_mode
        on stg_academic_record.tenant_code = detect_academic_record_mode.tenant_code
    where not is_single_year
    -- choose full academic record from most recent ODS/API year,
    -- per tenant, student, edorg
    qualify api_year = max(api_year) over(
        partition by stg_academic_record.tenant_code,
                     stg_academic_record.ed_org_id,
                     stg_academic_record.student_unique_id
        )
),
single_year_mode as (
    select stg_academic_record.*
    from stg_academic_record
    join detect_academic_record_mode
        on stg_academic_record.tenant_code = detect_academic_record_mode.tenant_code
    where detect_academic_record_mode.is_single_year
    and stg_academic_record.api_year = stg_academic_record.school_year
)
select * from longitudinal_mode
union all
select * from single_year_mode
