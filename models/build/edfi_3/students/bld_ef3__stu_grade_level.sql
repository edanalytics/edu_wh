with student_school as (
    select * from {{ ref('stg_ef3__student_school_associations') }}
),
xwalk_grade_levels as (
    select * from {{ ref('xwalk_grade_levels')}}
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
            exit_withdraw_date desc nulls first,
            -- tie break on grade level reverse alpha
            entry_grade_level desc
    )
),

-- In some implementations, enrolled grade level is not always equivalent to "true grade level".
optional_manual_override as (
    {% if not var('edu:stu_demos:grade_level_override', False) %}
        select * from find_grade_level
    {% else %}

        select
            tenant_code,
            k_student,
            school_year,
            coalesce(var('edu:stu_demos:grade_level_override')['where'], entry_grade_level) as entry_grade_level

        from find_grade_level as enrollment_source

            left join {{ ref(var('edu:stu_demos:grade_level_override')['source']) }} as override_source
            on enrollment_source.k_student = override_source.k_student

    {% endif %}
),

join_grade_integer as (
    select
        tenant_code,
        k_student,
        school_year,
        entry_grade_level,
        grade_level_integer 
    from optional_manual_override
    left join xwalk_grade_levels
        on find_grade_level.entry_grade_level = xwalk_grade_levels.grade_level
)
select * from join_grade_integer