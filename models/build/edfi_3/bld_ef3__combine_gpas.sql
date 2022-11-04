with current_gpas as (
    select * from {{ ref('stg_ef3__student_academic_records__gpas') }}
),
deprecated_gpas as (
    select * from {{ ref('stg_ef3__student_academic_records') }}
),
format_deprecated_gpas as (
    select 
        tenant_code,
        api_year,
        k_student_academic_record,
        'Unknown'      as gpa_type,
        cumulative_gpa as gpa_value,
        true           as is_cumulative,
        null           as max_gpa_value
    from deprecated_gpas
    union all 
    select 
        tenant_code,
        api_year,
        k_student_academic_record,
        'Unknown'      as gpa_type,
        session_gpa    as gpa_value,
        false          as is_cumulative,
        null           as max_gpa_value
    from deprecated_gpas
)
select * from current_gpas
union all
select * from format_deprecated_gpas