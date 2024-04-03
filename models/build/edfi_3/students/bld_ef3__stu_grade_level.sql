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
    {% if var('edu:stu_demos:grade_level_override', False) %}
        -- Expected YAML format:
        -- 'edu:stu_demos:grade_level_override':
        --     source: model_name
        --     where: column_select

        select
            enrollment_source.tenant_code,
            enrollment_source.k_student,
            enrollment_source.school_year,
            coalesce(
                {{ var('edu:stu_demos:grade_level_override')['where'] }}::string,
                enrollment_source.entry_grade_level
            ) as entry_grade_level

        from find_grade_level as enrollment_source

            -- Note, dbt test "grade_level_override_unique_on_k_student" is configured to fail if this override_source is not unique by k_student
            left join {{ ref(var('edu:stu_demos:grade_level_override')['source']) }} as override_source
            on enrollment_source.k_student = override_source.k_student

    {% else %}
        select * from find_grade_level

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
        on optional_manual_override.entry_grade_level = xwalk_grade_levels.grade_level
)
select * from join_grade_integer