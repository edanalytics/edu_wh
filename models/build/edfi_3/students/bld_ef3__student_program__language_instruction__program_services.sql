WITH stage_program_services AS (
    SELECT * FROM {{ ref('stg_ef3__stu_lang_instr__program_services') }}
),
ordered_services AS (
    SELECT
        tenant_code,
        api_year,
        k_student,
        k_student_xyear,
        k_program,
        k_lea,
        k_school,
        program_enroll_begin_date,
        program_service
    FROM stage_program_services
    ORDER BY program_service
),
wide AS (
    SELECT 
        tenant_code,
        api_year,
        k_student,
        k_student_xyear,
        k_program,
        k_lea,
        k_school,
        program_enroll_begin_date,
        ARRAY_AGG(program_service) AS program_services
    FROM ordered_services
    {{ dbt_utils.group_by(n=8) }}
)
SELECT * FROM wide