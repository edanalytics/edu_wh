{#-
    enrollment behavior is configurable because source systems do not all
    represent enrollment dates and withdrawal records the same way.

    these values control which enrollment records survive validation and how
    active enrollment status is calculated later in the model. they are resolved
    once here to keep the ctes below easier to read and understand.
-#}

    {#- controls whether the model removes exits before the first day of school. -#}
    {%- set exclude_exit_before_first_day = var('edu:enroll:exclude_exit_before_first_day', true) -%}

    {#- controls whether a student is still considered enrolled on exit_withdraw_date. -#}
    {%- set exit_date_inclusive = var('edu:enroll:exit_withdraw_date_inclusive', true) -%}

    {#-
        controls whether a student who exits on the first school day is treated
        as having enrolled that year or as exiting before enrollment began.
        defaults to the exit-date behavior above.
    -#}
    {%- set first_day_inclusive = var('edu:enroll:first_day_exit_date_inclusive', exit_date_inclusive) -%}

    {#-
        withdrawal codes that indicate the student was recorded as enrolled but never actually attended. 
        records with these codes are removed later.
    -#}
    {%- set excl_withdraw_codes = var('edu:enroll:exclude_withdraw_codes') -%}
    {%- if excl_withdraw_codes is string -%}
        {%- set excl_withdraw_codes = [excl_withdraw_codes] -%}
    {%- endif -%}

    {#-
        controls whether an enrollment is removed when the enrollment school year does not
        match the school year on the corresponding dim_student record.
    -#}
    {%- set exclude_cross_year = var('edu:enroll:exclude_cross_year_enrollments', false) -%}

    {#-
        when configured, students who exit within this many days of last_school_day
        can remain active through the end of that calendar year.

        example:
        - buffer_days = 5
        - last_school_day = 2026-06-15
        - exit_withdraw_date = 2026-06-12
        - result: the student qualifies for the year-end extension
    -#}
    {%- set buffer_days = var('edu:enroll:year_end_active_buffer_days', none) -%}


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

    -- some implementations do not link enrollments to a specific calendar because each school only has one. 
    -- when there is exactly one option, we can safely use it as a fallback.
    select * from dim_school_calendar
    qualify 1 = count(*) over(partition by k_school, school_year)

),

stu_school_records as (

    select
        -- select all enrollment columns except k_school_calendar because the calendar
        -- key is replaced below with the direct match or single-calendar fallback.
        {{ edu_edfi_source.star('stg_stu_school', except=['k_school_calendar']) }},

        dim_school.k_lea,

        -- prefer the calendar attached to the enrollment.
        -- if it is missing, use the school calendar only when there is one unambiguous option.
        coalesce(
            dim_school_calendar.k_school_calendar,
            single_calendar_schools.k_school_calendar
        ) as k_school_calendar,

        bld_school_calendar_windows.first_school_day,
        bld_school_calendar_windows.last_school_day,

        -- these flags will help us identify which enrollment records to keep
        -- when calculating active enrollments.

        -- flag 1: 
            -- keep the enrollment only if the exit date is on or after the first school day.
            -- if no calendar is available, keep the record because there is no
            -- reliable first school day to compare against.
        (
            {{ date_within_end_date(
                'bld_school_calendar_windows.first_school_day',
                'stg_stu_school.exit_withdraw_date',
                first_day_inclusive
            ) }}
            or bld_school_calendar_windows.first_school_day is null
        ) as is_exit_on_or_after_first_day,

        -- flag 2:
            -- keep the enrollment only if it does not end before its entry date.
            -- example:
                -- valid:   entry_date = 08/20/2026 and exit_withdraw_date = 08/20/2026.
                -- invalid: entry_date = 08/20/2026 and exit_withdraw_date = 08/10/2026.
        (
            stg_stu_school.exit_withdraw_date is null
            or stg_stu_school.exit_withdraw_date >= stg_stu_school.entry_date
        ) as is_exit_on_or_after_entry,

        -- flag 3:
            -- keep the enrollment only if its withdrawal code does not indicate that the student never actually enrolled.
        {%- if excl_withdraw_codes | length %}
        (
            stg_stu_school.exit_withdraw_type not in (
                '{{ excl_withdraw_codes | join("', '") }}'
            )
            or stg_stu_school.exit_withdraw_type is null
        ) as has_retained_withdraw_type
        {%- else %}
        true as has_retained_withdraw_type
        {%- endif %},

        -- flag 4:
            -- keep the enrollment only if its school_year matches the school_year on
            -- the student record joined by k_student.
        dim_student.school_year = stg_stu_school.school_year as is_same_year_as_student_record

    from stg_stu_school
    -- student and school records are required, so these joins also filter.
    join dim_student
        on stg_stu_school.k_student = dim_student.k_student
    join dim_school
        on stg_stu_school.k_school = dim_school.k_school
    -- these are left joins because the calendar may not be available for every enrollment record.
    left join dim_school_calendar
        on stg_stu_school.k_school_calendar = dim_school_calendar.k_school_calendar
    left join single_calendar_schools
        on stg_stu_school.k_school = single_calendar_schools.k_school
        and stg_stu_school.school_year = single_calendar_schools.school_year
    -- calendar windows only use the direct calendar match, not the fallback.
    left join bld_school_calendar_windows
        on stg_stu_school.k_school = bld_school_calendar_windows.k_school
        and stg_stu_school.school_year = bld_school_calendar_windows.school_year
        and equal_null(
            dim_school_calendar.k_school_calendar,
            bld_school_calendar_windows.k_school_calendar
        )

),

valid_stu_school_records as (

    -- apply the validation flags created above to decide which enrollment records continue through the model. 
    -- the entry/exit check is always required, while the other checks only apply when enabled by configuration.
    -- once the filtering is done, remove the helper flags because they are no longer needed downstream.
    select
        {{ edu_edfi_source.star('stu_school_records', except=[
            'is_exit_on_or_after_first_day',
            'is_exit_on_or_after_entry',
            'has_retained_withdraw_type',
            'is_same_year_as_student_record'
        ]) }}
    from stu_school_records
    where is_exit_on_or_after_entry

    {%- if exclude_exit_before_first_day %}
        and is_exit_on_or_after_first_day
    {%- endif %}

    {%- if excl_withdraw_codes | length %}
        and has_retained_withdraw_type
    {%- endif %}

    {%- if exclude_cross_year %}
        and is_same_year_as_student_record
    {%- endif %}

),

enrollment_status_flags as (

    select
        *,

        -- identify the school year that should currently count as active for each tenant.
        -- only consider school years whose first school day has already passed.
        -- example: 
            -- if 2027 enrollment records are loaded in july 2026,
            -- but the 2027 school year starts in august, 2026 remains the active school year until then.
        school_year = max(
            iff(
                coalesce(first_school_day, entry_date) <= current_date(),
                school_year,
                null
            )
        ) over(partition by tenant_code) as is_active_school_year,

        -- identify whether the enrollment has started as of today.
        -- future enrollment records remain false until entry_date is reached.
        entry_date <= current_date() as has_enrollment_started,

        -- identify whether the enrollment should still count as active based on its exit date.
        -- normally, the student stops counting as active once exit_withdraw_date is reached,
        -- depending on the configured exit-date inclusivity.
        -- when a year-end buffer is configured, students who exit near last_school_day
        -- can remain active through the end of that calendar year.
        (
            {{ date_within_end_date(
                'current_date()',
                'exit_withdraw_date',
                exit_date_inclusive
            ) }}
            {%- if buffer_days is not none %}
            or (
                -- negate in jinja so a negative value cannot render as "--5",
                -- which sql would interpret as the start of a comment.
                exit_withdraw_date
                    >= dateadd(day, {{ -buffer_days }}, last_school_day)
                and year(last_school_day) = year(current_date())
            )
            {%- endif %}
        ) as is_within_effective_end_date

    from valid_stu_school_records

)

select * from enrollment_status_flags