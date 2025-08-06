{{
  config(
    post_hook=[
        "alter table {{ this }} alter column k_student set not null",
        "alter table {{ this }} alter column k_school set not null",
        "alter table {{ this }} alter column entry_date set not null",
        "alter table {{ this }} add primary key (k_student, k_school, entry_date)",
        "alter table {{ this }} add constraint fk_{{ this.name }}_student foreign key (k_student) references {{ ref('dim_student') }}",
        "alter table {{ this }} add constraint fk_{{ this.name }}_school foreign key (k_school) references {{ ref('dim_school') }}",
    ]
  )
}}

with stg_stu_school as (
    select * from {{ ref('stg_ef3__student_school_associations') }}
),
dim_student as (
    select * from {{ ref('dim_student') }}
),
dim_school as (
    select * from {{ ref('dim_school') }}
),
dim_school_calendar as (
    select * from {{ ref('dim_school_calendar') }}
),
dim_calendar_date as (
    select * from {{ ref('dim_calendar_date') }}
),
bld_school_calendar_windows as (
    select * from {{ ref('bld_ef3__school_calendar_windows') }}
),
xwalk_grade_levels as (
    select * from {{ ref('xwalk_grade_levels')}}
),
single_calendar_schools as (
    -- some implementations may not provide a school calendar link because
    -- their system only allows one calendar per school anyway.
    -- we will detect these cases and fill in missing school calendars
    -- only when doing so is unambiguous
    select * from dim_school_calendar
    qualify 1 = count(*) over(partition by k_school, school_year)
),
formatted as (
    select 
        dim_student.k_student,
        dim_student.k_student_xyear,
        dim_school.k_lea,
        dim_school.k_school,
        coalesce(
            dim_school_calendar.k_school_calendar,
            single_calendar_schools.k_school_calendar
        ) as k_school_calendar,
        stg_stu_school.tenant_code,
        stg_stu_school.school_year,
        stg_stu_school.entry_date,
        stg_stu_school.exit_withdraw_date,
        stg_stu_school.is_primary_school,
        stg_stu_school.is_repeat_grade,
        stg_stu_school.is_school_choice_transfer,
        stg_stu_school.is_school_choice,
        stg_stu_school.school_choice_basis,
        stg_stu_school.enrollment_type,
        -- create indicator for active enrollment
        iff(
            -- is highest school year observed by tenant
            stg_stu_school.school_year = max(stg_stu_school.school_year) 
                over(partition by stg_stu_school.tenant_code)
            -- not yet exited
            and (exit_withdraw_date is null
                or exit_withdraw_date >= current_date())
            -- enrollment has begun
            and entry_date <= current_date(),
            true, false
        ) as is_active_enrollment,
        stg_stu_school.entry_grade_level,
        xwalk_grade_levels.grade_level_integer,
        stg_stu_school.entry_grade_level_reason,
        stg_stu_school.entry_type,
        stg_stu_school.exit_withdraw_type,
        stg_stu_school.class_of_school_year,
        stg_stu_school.next_year_school_id,
        stg_stu_school.next_year_grade_level,
        stg_stu_school.k_graduation_plan,
        stg_stu_school.graduation_plan_type,
        stg_stu_school.v_alternative_graduation_plans,
        stg_stu_school.v_education_plans,
        stg_stu_school.residency_status,
        -- column to choose the latest record for multiple enrollments 
        -- at the same school in the same year
        -- note: difficult column to name without implying it has cross-year
        -- or cross-school meaning
        stg_stu_school.entry_date = max(stg_stu_school.entry_date) over(
            partition by stg_stu_school.k_student, stg_stu_school.k_school
        ) as is_latest_annual_entry
        {# add any extension columns configured from stg_ef3__student_school_associations #}
        {{ edu_edfi_source.extract_extension(model_name='stg_ef3__student_school_associations', flatten=False) }}
    from stg_stu_school
    join dim_student
        on stg_stu_school.k_student = dim_student.k_student
    join dim_school
        on stg_stu_school.k_school = dim_school.k_school
    left join dim_school_calendar
        on stg_stu_school.k_school_calendar = dim_school_calendar.k_school_calendar
    left join single_calendar_schools
        on stg_stu_school.k_school = single_calendar_schools.k_school
        and stg_stu_school.school_year = single_calendar_schools.school_year
    left join bld_school_calendar_windows
        on stg_stu_school.k_school = bld_school_calendar_windows.k_school
        and stg_stu_school.school_year = bld_school_calendar_windows.school_year
        and equal_null(dim_school_calendar.k_school_calendar, bld_school_calendar_windows.k_school_calendar)
    left join xwalk_grade_levels
        on stg_stu_school.entry_grade_level = xwalk_grade_levels.grade_level
    where true
   {% if var('edu:enroll:exclude_exit_before_first_day', True) -%}
      -- exclude students who exited before the first school day
      and (exit_withdraw_date >= bld_school_calendar_windows.first_school_day
          or exit_withdraw_date is null
          or bld_school_calendar_windows.first_school_day is null
          )
    {% endif %}
    -- exclude students whose exit day is before their entry day
    and (exit_withdraw_date >= entry_date
        or exit_withdraw_date is null)
    -- exclude students who never actually enrolled
    {% set excl_withdraw_codes =  var('edu:enroll:exclude_withdraw_codes')  %}
    {% if excl_withdraw_codes | length -%}
      {% if excl_withdraw_codes is string -%}
        {% set excl_withdraw_codes = [excl_withdraw_codes] %}
      {%- endif -%}
      -- drop invalid enrollments
      and (stg_stu_school.exit_withdraw_type not in (
      '{{ excl_withdraw_codes | join("', '") }}'
      )
      -- keep rows with no exit_withdraw
      or stg_stu_school.exit_withdraw_type is null) 
    {% endif %}
    {% if var('edu:enroll:exclude_cross_year_enrollments', False)%}
    and dim_student.school_year = stg_stu_school.school_year
    {% endif %}
)
select * from formatted
