{# customizable: certain program names may be excluded and not counted as special education #}
{% set exclude_programs = var('edu:special_ed:exclude_programs') %}

{# customizable: the column that defines the start date for the special education program #}
{% set start_date_column = var('edu:special_ed:start_date_column') %}

{# customizable: the column that defines the end date for the special education program #}
{% set exit_date_column = var('edu:special_ed:exit_date_column') %}

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
          and {{ start_date_column }} <= current_date() -- start date is today or in the past
          and ({{ exit_date_column }} is null -- no exit date
            or {{ exit_date_column }} > current_date()) -- exit date is in the future
        ) as is_special_education_active -- if the student has an active special education program enrollment
    from stg_spec_ed
    group by 1
)
select * from maxed