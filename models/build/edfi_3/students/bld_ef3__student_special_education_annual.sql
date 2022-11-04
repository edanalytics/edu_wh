{# customizable: certain program names may be excluded and not counted as special education #}
{% set exclude_programs = var('edu:special_ed:exclude_programs')  %}

with stg_spec_ed as (
    select * from {{ ref('stg_ef3__student_special_education_program_associations') }}
),
maxed as (
    -- take one row per student, maxing across kept rows
    select 
        k_student,
        any_value(tenant_code) as tenant_code,
        max(
          {{ value_not_in_list(field='program_name', excluded_items=exclude_programs) }}
        ) as is_special_education_annual, -- the student had a special education program enrollment any time during the year
        max(is_idea_eligible) as is_idea_eligible,
        max(is_multiply_disabled) as is_multiply_disabled
    from stg_spec_ed
    group by 1
)
select * from maxed