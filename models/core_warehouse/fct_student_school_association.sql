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

with bld_stu_sch_assoc_flags as (
    select * from {{ ref('bld_ef3__stu_sch_assoc__enrollment_flags') }}
),

xwalk_grade_levels as (
    select * from {{ ref('xwalk_grade_levels') }}
),

formatted as (
    select
        bld_stu_sch_assoc_flags.k_student,
        bld_stu_sch_assoc_flags.k_student_xyear,
        bld_stu_sch_assoc_flags.k_lea,
        bld_stu_sch_assoc_flags.k_school,
        bld_stu_sch_assoc_flags.k_school_calendar,
        bld_stu_sch_assoc_flags.tenant_code,
        bld_stu_sch_assoc_flags.school_year,
        bld_stu_sch_assoc_flags.entry_date,
        bld_stu_sch_assoc_flags.exit_withdraw_date,
        bld_stu_sch_assoc_flags.is_primary_school,
        bld_stu_sch_assoc_flags.is_repeat_grade,
        bld_stu_sch_assoc_flags.is_school_choice_transfer,
        bld_stu_sch_assoc_flags.is_school_choice,
        bld_stu_sch_assoc_flags.school_choice_basis,
        bld_stu_sch_assoc_flags.enrollment_type,

        -- an enrollment is active only when all three conditions are true:
            -- the enrollment's school_year is the most recent school year that is actively in session for that tenant,
            -- the enrollment entry date has been reached as of today,
            -- the enrollment has not reached its effective end date.
        -- coalesce null to false so an unknown component does not result in
        -- a null active-enrollment flag.
        coalesce(
            bld_stu_sch_assoc_flags.is_active_school_year
            and bld_stu_sch_assoc_flags.has_enrollment_started
            and bld_stu_sch_assoc_flags.is_within_effective_end_date,
            false
        ) as is_active_enrollment,

        bld_stu_sch_assoc_flags.entry_grade_level,
        xwalk_grade_levels.grade_level_integer,
        bld_stu_sch_assoc_flags.entry_grade_level_reason,
        bld_stu_sch_assoc_flags.entry_type,
        bld_stu_sch_assoc_flags.exit_withdraw_type,
        bld_stu_sch_assoc_flags.class_of_school_year,
        bld_stu_sch_assoc_flags.next_year_school_id,
        bld_stu_sch_assoc_flags.next_year_grade_level,
        bld_stu_sch_assoc_flags.k_graduation_plan,
        bld_stu_sch_assoc_flags.graduation_plan_type,
        bld_stu_sch_assoc_flags.v_alternative_graduation_plans,
        bld_stu_sch_assoc_flags.v_education_plans,
        bld_stu_sch_assoc_flags.residency_status,

        -- column to choose the latest record for multiple enrollments
        -- at the same school in the same year
        -- note: difficult column to name without implying it has cross-year
        -- or cross-school meaning
        bld_stu_sch_assoc_flags.entry_date = max(bld_stu_sch_assoc_flags.entry_date) over(
            partition by bld_stu_sch_assoc_flags.k_student, bld_stu_sch_assoc_flags.k_school
        ) as is_latest_annual_entry

        {#- add any extension columns configured from stg_ef3__student_school_associations #}
        {{ edu_edfi_source.extract_extension(model_name='stg_ef3__student_school_associations', flatten=False) }}

    from bld_stu_sch_assoc_flags
    left join xwalk_grade_levels
        on bld_stu_sch_assoc_flags.entry_grade_level = xwalk_grade_levels.grade_level
)
select * from formatted
