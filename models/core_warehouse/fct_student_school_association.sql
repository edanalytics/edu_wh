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

with bld_enrollment_flags as (
    -- valid enrollment records only: the build model applies the
    -- enrollment-validity filters, resolves k_school_calendar, and computes
    -- the component status flags
    select * from {{ ref('bld_ef3__stu_sch_assoc__enrollment_flags') }}
),
xwalk_grade_levels as (
    select * from {{ ref('xwalk_grade_levels') }}
),
formatted as (
    select
        bld_enrollment_flags.k_student,
        bld_enrollment_flags.k_student_xyear,
        bld_enrollment_flags.k_lea,
        bld_enrollment_flags.k_school,
        bld_enrollment_flags.k_school_calendar,
        bld_enrollment_flags.tenant_code,
        bld_enrollment_flags.school_year,
        bld_enrollment_flags.entry_date,
        bld_enrollment_flags.exit_withdraw_date,
        bld_enrollment_flags.is_primary_school,
        bld_enrollment_flags.is_repeat_grade,
        bld_enrollment_flags.is_school_choice_transfer,
        bld_enrollment_flags.is_school_choice,
        bld_enrollment_flags.school_choice_basis,
        bld_enrollment_flags.enrollment_type,
        -- create indicator for active enrollment. coalesce so that a null
        -- component reads as inactive rather than null, matching the
        -- behavior of the iff() this replaced.
        coalesce(
            -- is the newest school year that has actually begun for this tenant
            bld_enrollment_flags.is_active_school_year
            -- enrollment has begun
            and bld_enrollment_flags.has_enrollment_started
            -- not yet exited, or 'year-end extension' if configured
            and bld_enrollment_flags.is_within_effective_end_date,
            false
        ) as is_active_enrollment,
        bld_enrollment_flags.entry_grade_level,
        xwalk_grade_levels.grade_level_integer,
        bld_enrollment_flags.entry_grade_level_reason,
        bld_enrollment_flags.entry_type,
        bld_enrollment_flags.exit_withdraw_type,
        bld_enrollment_flags.class_of_school_year,
        bld_enrollment_flags.next_year_school_id,
        bld_enrollment_flags.next_year_grade_level,
        bld_enrollment_flags.k_graduation_plan,
        bld_enrollment_flags.graduation_plan_type,
        bld_enrollment_flags.v_alternative_graduation_plans,
        bld_enrollment_flags.v_education_plans,
        bld_enrollment_flags.residency_status,
        -- column to choose the latest record for multiple enrollments
        -- at the same school in the same year
        -- note: difficult column to name without implying it has cross-year
        -- or cross-school meaning
        bld_enrollment_flags.entry_date = max(bld_enrollment_flags.entry_date) over(
            partition by bld_enrollment_flags.k_student, bld_enrollment_flags.k_school
        ) as is_latest_annual_entry
        {# add any extension columns configured from stg_ef3__student_school_associations #}
        {{ edu_edfi_source.extract_extension(model_name='stg_ef3__student_school_associations', flatten=False) }}
    from bld_enrollment_flags
    left join xwalk_grade_levels
        on bld_enrollment_flags.entry_grade_level = xwalk_grade_levels.grade_level
)
select * from formatted
