with course_transcripts as (
    select * from {{ ref('fct_course_transcripts') }}
),

dim_course as (
    select * from {{ ref('dim_course') }}
),

final as (

    select
        course_transcripts.k_student_xyear,
        course_transcripts.tenant_code,
        dim_course.course_code,
        max(course_transcripts.school_year) as completed_school_year,
        -- metadata from the latest completion year
        max_by(course_transcripts.k_course, course_transcripts.school_year) as k_course__completed_year,
        max_by(dim_course.course_title, course_transcripts.school_year) as course_title__completed_year
    from course_transcripts
    join dim_course
        on course_transcripts.k_course = dim_course.k_course
    where course_transcripts.course_attempt_result in ('{{ var("edu:course_transcripts:passing_results", ["P"]) | join("', '") }}')
    group by 1, 2, 3

)

select * from final
