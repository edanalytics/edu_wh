with fct_student_diploma as (
    select * from {{ ref('fct_student_diploma') }}
), 
count_duplicates as (
    select
        tenant_code,
        k_student,
        k_student_xyear,
        k_lea,
        k_school,
        school_year,
        academic_term,
        diploma_type,
        diploma_award_date,
        count(*) over (partition by k_student, k_student_xyear, school_year, k_lea, k_school, diploma_type, diploma_award_date) as n_duplicates,
        row_number() over (partition by k_student, k_student_xyear, school_year, k_lea, k_school, diploma_type, diploma_award_date
            order by {% if var('edu:xwalk_academic_terms:enabled', False) %} coalesce(sort_index, 99) {% else %} academic_term {% endif %}) = 1 as is_kept_in_fct_student_diploma
    from fct_student_diploma
)
select * 
from count_duplicates
where n_duplicates > 1
order by tenant_code, k_student