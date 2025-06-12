{{
  config(
    post_hook=[
        "alter table {{ this }} alter column k_learning_standard set not null",
        "alter table {{ this }} add primary key (k_learning_standard)"
    ]
  )
}}


with stg_learning_standards as (
    select * from {{ ref('stg_ef3__learning_standards') }}
),
formatted as (
    select 
        stg_learning_standards.k_learning_standard,
        stg_learning_standards.k_learning_standard__parent,
        stg_learning_standards.tenant_code,
        stg_learning_standards.api_year as school_year,
        stg_learning_standards.learning_standard_id,
        stg_learning_standards.learning_standard_item_code,
        stg_learning_standards.course_title, 
        stg_learning_standards.learning_standard_description,
        stg_learning_standards.learning_standard_category,
        stg_learning_standards.learning_standard_scope,
        stg_learning_standards.success_criteria,
        stg_learning_standards.namespace,
        stg_learning_standards.uri,
        stg_learning_standards.v_academic_subjects,
        stg_learning_standards.v_grade_levels,
        stg_learning_standards.v_learning_standard_identification_codes,
        stg_learning_standards.v_content_standard
    from stg_learning_standards
)
select * from formatted
order by tenant_code, k_learning_standard