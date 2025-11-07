with stage_program_services as (
    select * from {{ ref('stg_ef3__stu_migrant_edu__program_services') }}
),

wide as (
    select 
        tenant_code,
        api_year,
        k_student,
        k_student_xyear,
        k_program,
        k_lea,
        k_school,
        program_enroll_begin_date,
        {{ edu_edfi_source.json_array_agg('program_service', order_by='program_service', is_terminal=True) }} as program_services

    from stage_program_services

    {{ dbt_utils.group_by(n=8) }}
)

select * from wide
