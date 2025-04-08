with student_cohort_years as (
    select * from {{ ref('stg_ef3__stu_ed_org__cohort_years') }}
),
build_object as (
    select 
        tenant_code,
        api_year,
        k_student,
        {{
            json_array_agg(
                json_object_construct(
                    [['cohort_year_type', 'cohort_year_type'],
                     ['school_year', 'school_year'],
                     ['academic_term', 'academic_term']]
                ),
            is_terminal=True)
        }} as cohort_year_array
    from student_cohort_years
    group by 1,2,3
)
select * from build_object