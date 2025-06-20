with stg_course_subjects as (
    select * from {{ ref('stg_ef3__courses__academic_subjects') }}
),
stg_courses as (
    select * from {{ ref('stg_ef3__courses') }}
),
build_array as (
    select
        tenant_code,
        api_year,
        k_course,
        array_agg(academic_subject) as subject_array
    from stg_course_subjects
    group by 1, 2, 3
)

select
    stg_courses.tenant_code,
    stg_courses.api_year,
    stg_courses.k_course,
    build_array.subject_array,
    coalesce(
        case
            when array_size(build_array.subject_array) = 1
                then subject_array[0]
            when array_size(build_array.subject_array) > 1
                then 'Multiple'
        end,
        stg_courses.academic_subject
    ) as academic_subject
from stg_courses 
left join build_array 
    on stg_courses.k_course = build_array.k_course

