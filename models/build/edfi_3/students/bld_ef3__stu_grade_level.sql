with student_school as (
    select * from {{ ref('stg_ef3__student_school_associations') }}
),
find_grade_level as (
    select 
        tenant_code,
        k_student,
        school_year,
        entry_grade_level
    from student_school
    qualify 1 = row_number() over(
        partition by k_student, school_year
        order by 
            -- latest entry date
            entry_date desc,
            -- tie break on longer
            (coalesce(exit_withdraw_date, current_date()) - entry_date) desc,
            -- tie break on grade level reverse alpha
            entry_grade_level desc
    )
)
select * from find_grade_level
