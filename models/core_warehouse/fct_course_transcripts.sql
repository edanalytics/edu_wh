-- SPDX-FileCopyrightText: 2022 Education Analytics
-- SPDX-License-Identifier: MPL-2.0

{{
  config(
    post_hook=[
        "alter table {{ this }} alter column k_course set not null",
        "alter table {{ this }} alter column k_course_transcript set not null",
        "alter table {{ this }} add primary key (k_course_transcript)",
        "alter table {{ this }} add constraint fk_{{ this.name }}_course foreign key (k_course) references {{ ref('dim_course') }}",
        "alter table {{ this }} add constraint fk_{{ this.name }}_student foreign key (k_student) references {{ ref('dim_student') }}",
    ]
  )
}}

with course_transcripts as (
    select * from {{ ref('bld_ef3__course_transcripts_latest_ods') }}
),
dim_course as (
    select * from {{ ref('dim_course') }}
),

dim_student as (
    select * from {{ ref('dim_student') }}
),
-- Transcripts choose their ODS separately from academic records, so a transcript's record
-- may not be in fct_student_academic_record (e.g. the newer ODS recoded the record's term).
-- Student, ed org, and term columns come from the transcript's own record reference instead.
-- For the same reason, k_student_academic_record is left out, since it may not match a row in fct_student_academic_record.
formatted as (
    select
        -- Give each transcript line its own key using tenant, student, school year, record ed org, term,
        -- course, and attempt result. Leave out api_year so receiving the same line in a newer ODS doesn't change its key.
        -- For the ed org, use k_school for school-level records and k_lea for LEA-level records,
        -- so the key doesn't change if a school's district changes.
        -- Keep this key built the same way as k_student_gpa in fct_student_gpa.
        {{ dbt_utils.generate_surrogate_key(
            [
                'course_transcripts.tenant_code',
                'course_transcripts.k_student_xyear',
                'course_transcripts.school_year',
                'coalesce(course_transcripts.k_school, course_transcripts.k_lea)',
                'course_transcripts.course_ed_org_id',
                'lower(course_transcripts.academic_term)',
                'lower(course_transcripts.course_code)',
                'lower(course_transcripts.course_attempt_result)'
            ]
        ) }} as k_course_transcript,
        course_transcripts.k_course,
        course_transcripts.k_lea,
        course_transcripts.k_school,
        dim_student.k_student,  -- will be null if no dim_student record for this school year
        course_transcripts.k_student_xyear,
        course_transcripts.tenant_code,
        course_transcripts.school_year,
        course_transcripts.academic_term,
        course_transcripts.course_attempt_result,
        coalesce(course_transcripts.course_title, dim_course.course_title) as course_title,
        course_transcripts.alternative_course_code,
        course_transcripts.alternative_course_title,
        course_transcripts.when_taken_grade_level,
        course_transcripts.final_letter_grade_earned,
        course_transcripts.final_numeric_grade_earned,
        course_transcripts.earned_credits,
        course_transcripts.attempted_credits,
        course_transcripts.course_repeat_code,
        course_transcripts.method_credit_earned,
        course_transcripts.earned_credit_type,
        course_transcripts.earned_credit_conversion,
        course_transcripts.attempted_credit_type,
        course_transcripts.attempted_credit_conversion,
        course_transcripts.assigning_organization_identification_code,
        course_transcripts.course_catalog_url,
        course_transcripts.external_ed_org_id,
        course_transcripts.external_ed_org_name,
        course_transcripts.v_earned_additional_credits,
        course_transcripts.v_credit_categories,
        course_transcripts.v_academic_subjects,
        course_transcripts.v_alternative_course_identification_codes,
        course_transcripts.v_programs,
        course_transcripts.v_sections,
        course_transcripts.v_partial_course_transcript_awards
        {# add any extension columns configured from stg_ef3__course_transcripts #}
        {{ edu_edfi_source.extract_extension(model_name='stg_ef3__course_transcripts', flatten=False) }}
    from course_transcripts
    join dim_course
        on course_transcripts.k_course = dim_course.k_course
    left join dim_student
        on course_transcripts.k_student_xyear = dim_student.k_student_xyear
        and course_transcripts.school_year = dim_student.school_year
)
select * from formatted
