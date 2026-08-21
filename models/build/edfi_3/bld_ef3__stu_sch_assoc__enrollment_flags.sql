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

bld_school_calendar_windows as (
    select * from {{ ref('bld_ef3__school_calendar_windows') }}
),

single_calendar_schools as (
    -- some implementations may not provide a school calendar link because
    -- their system only allows one calendar per school anyway.
    -- we will detect these cases and fill in missing school calendars
    -- only when doing so is unambiguous
    select * from dim_school_calendar
    qualify 1 = count(*) over(partition by k_school, school_year)
),

valid_stu_school_records as (

    select
        -- exclude the raw calendar reference so the resolved value below
        -- can take over the k_school_calendar name
        {{ edu_edfi_source.star('stg_stu_school', except=['k_school_calendar']) }},

        dim_school.k_lea,

        -- the enrollment's raw calendar reference, resolved: the direct match
        -- against dim_school_calendar when it exists, otherwise the school's
        -- only calendar when that choice is unambiguous
        coalesce(
            dim_school_calendar.k_school_calendar,
            single_calendar_schools.k_school_calendar
        ) as k_school_calendar,

        -- first_school_day to decide whether a school year has begun,
        -- last_school_day for the year-end buffer check
        bld_school_calendar_windows.first_school_day,
        bld_school_calendar_windows.last_school_day

    from stg_stu_school
    -- these joins are filters as much as lookups: an enrollment with no
    -- matching student or school row is dropped, and must be dropped here so
    -- it does not vote in the is_active_school_year window below
    join dim_student
        on stg_stu_school.k_student = dim_student.k_student
    join dim_school
        on stg_stu_school.k_school = dim_school.k_school
    left join dim_school_calendar
        on stg_stu_school.k_school_calendar = dim_school_calendar.k_school_calendar
    left join single_calendar_schools
        on stg_stu_school.k_school = single_calendar_schools.k_school
        and stg_stu_school.school_year = single_calendar_schools.school_year
    -- the windows join keys on the direct calendar match only, never the
    -- single-calendar fallback
    left join bld_school_calendar_windows
        on stg_stu_school.k_school = bld_school_calendar_windows.k_school
        and stg_stu_school.school_year = bld_school_calendar_windows.school_year
        and equal_null(
            dim_school_calendar.k_school_calendar,
            bld_school_calendar_windows.k_school_calendar
        )

    where true
    {%- if var('edu:enroll:exclude_exit_before_first_day', True) %}
      {#- allow implementations with exclusive exit dates to still keep first-day exits -#}
      {%- set first_day_inclusive = var('edu:enroll:first_day_exit_date_inclusive', var('edu:enroll:exit_withdraw_date_inclusive', True)) %}
        -- exclude students who exited before the first school day
        and (
            {{ date_within_end_date(
                'bld_school_calendar_windows.first_school_day',
                'stg_stu_school.exit_withdraw_date',
                first_day_inclusive
            ) }}
            or bld_school_calendar_windows.first_school_day is null
        )
    {%- endif %}
        -- make sure exit occurred on or after entry
        and (
            stg_stu_school.exit_withdraw_date is null
            or stg_stu_school.exit_withdraw_date >= stg_stu_school.entry_date
        )
    {%- set excl_withdraw_codes = var('edu:enroll:exclude_withdraw_codes') %}
    {%- if excl_withdraw_codes is string %}
      {%- set excl_withdraw_codes = [excl_withdraw_codes] %}
    {%- endif %}
    {%- if excl_withdraw_codes | length %}
        -- drop invalid enrollments while retaining rows without an exit type
        and (
            stg_stu_school.exit_withdraw_type not in (
                '{{ excl_withdraw_codes | join("', '") }}'
            )
            or stg_stu_school.exit_withdraw_type is null
        )
    {%- endif %}
    {%- if var('edu:enroll:exclude_cross_year_enrollments', False) %}
        -- drop enrollments whose year disagrees with the student record's year
        and dim_student.school_year = stg_stu_school.school_year
    {%- endif %}

),

enrollment_status_flags as (

    select
        *,

        -- is this row's school year the newest one that has actually begun
        -- for this tenant? falls back to entry_date when the calendar
        -- didn't match, so tenants with missing calendar data aren't
        -- stranded. using the newest year *loaded* would strand current
        -- enrollments whenever next year's data is ingested early.
        school_year = max(
            iff(
                coalesce(first_school_day, entry_date) <= current_date(),
                school_year,
                null
            )
        ) over (
            partition by tenant_code
        ) as is_active_school_year,

        entry_date <= current_date() as has_enrollment_started,

        -- is today still within this enrollment's end boundary? normally that
        -- boundary is exit_withdraw_date, but when the year-end buffer is
        -- configured a student who exited within buffer_days of
        -- last_school_day also counts, until the calendar year rolls over.
        (
            {{ date_within_end_date(
                'current_date()',
                'exit_withdraw_date',
                var('edu:enroll:exit_withdraw_date_inclusive', True)
            ) }}
            {%- set buffer_days = var('edu:enroll:year_end_active_buffer_days', none) %}
            {%- if buffer_days is not none %}
            or (
                -- jinja negates, rather than emitting a literal "-" prefix:
                -- a negative buffer_days would otherwise render as "--5",
                -- which SQL reads as the start of a line comment
                exit_withdraw_date
                    >= dateadd(day, {{ -buffer_days }}, last_school_day)
                and year(last_school_day) = year(current_date())
            )
            {%- endif %}
        ) as is_within_effective_end_date

    from valid_stu_school_records

)

select * from enrollment_status_flags
