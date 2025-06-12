with current_gpas as (
    select * from {{ ref('stg_ef3__student_academic_records__gpas') }}
),
deprecated_gpas as (
    select * from {{ ref('stg_ef3__student_academic_records') }}
),
format_current_gpas as (
    select
        tenant_code,
        api_year,
        k_student_academic_record,
        gpa_type,
        gpa_value,
        is_cumulative,
        max_gpa_value
    from current_gpas
),
format_deprecated_gpas as (
    select 
        tenant_code,
        api_year,
        k_student_academic_record,
        'Cumulative, unknown weighting' as gpa_type,
        cumulative_gpa as gpa_value,
        true           as is_cumulative,
        null           as max_gpa_value
    from deprecated_gpas
    union all 
    select 
        tenant_code,
        api_year,
        k_student_academic_record,
        'Non-cumulative, unknown weighting' as gpa_type,
        session_gpa as gpa_value,
        false       as is_cumulative,
        null        as max_gpa_value
    from deprecated_gpas
)
select * from format_current_gpas
union all
select * from format_deprecated_gpas
