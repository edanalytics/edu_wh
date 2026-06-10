{{
  config(
    post_hook=[
        "alter table {{ this }} alter column k_course set not null",
        "alter table {{ this }} add primary key (k_course)",
    ]
  )
}}


with stg_course as (
    select * from {{ ref('stg_ef3__courses') }}
),
bld_ef3__wide_ids_course as (
    select * from {{ ref('bld_ef3__wide_ids_course') }}
),
bld_ef3__course_subject as (
    select * from {{ ref('bld_ef3__course_subject') }}
),
formatted as (
    select 
        stg_course.k_course,
        stg_course.tenant_code,
        stg_course.school_year,
        stg_course.course_code,
        stg_course.course_title,
        {{ accordion_columns(
            source_table='bld_ef3__wide_ids_course', 
            exclude_columns=['tenant_code', 'api_year', 'k_course']) }}
        stg_course.course_description,
        stg_course.k_lea,
        stg_course.k_school,
        stg_course.ed_org_id,
        stg_course.ed_org_type,
        bld_ef3__course_subject.academic_subject,
        stg_course.career_pathway,
        stg_course.course_defined_by,
        stg_course.gpa_applicability,
        stg_course.date_course_adopted,
        stg_course.is_high_school_course_requirement,
        stg_course.max_completions_for_credit,
        stg_course.maximum_available_credits,
        stg_course.maximum_available_credit_type,
        stg_course.maximum_available_credit_conversion,
        stg_course.minimum_available_credits,
        stg_course.minimum_available_credit_type,
        stg_course.minimum_available_credit_conversion,
        stg_course.number_of_parts,
        stg_course.time_required_for_completion,
        bld_ef3__course_subject.subject_array

    from stg_course
    left join bld_ef3__wide_ids_course 
        on stg_course.k_course = bld_ef3__wide_ids_course.k_course
    left join bld_ef3__course_subject
        on stg_course.k_course = bld_ef3__course_subject.k_course
    
)
{{ add_custom_data_source('edu:course:custom_data_sources', join_cols=['k_course']) }}
select * from add_custom_data_source
order by tenant_code, school_year desc, k_course
