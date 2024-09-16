with student_cohort_years as (
    select * from {{ ref('stg_ef3__stu_ed_org__cohort_years') }}
),
build_object as (
    select 
        tenant_code,
        api_year,
        k_student,
        array_agg(object_construct('cohort_year', cohort_year, 
                                    'school_year', school_year,
                                    'term', term)) as cohort_year_array
    from student_cohort_years
    group by 1,2,3
)
select * from build_object