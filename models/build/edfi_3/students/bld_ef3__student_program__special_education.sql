{# customizable: certain program names may be excluded and not counted as language instruction #}
{% set exclude_programs = var('edu:special_ed:exclude_programs', []) %}

{# customizable: the column that defines the start date for the language instruction program #}
{% set start_date_column = var('edu:special_ed:start_date_column', 'program_enroll_begin_date') %}

{# customizable: the column that defines the end date for the language instruction program #}
{% set exit_date_column = var('edu:special_ed:exit_date_column', 'program_enroll_begin_date') %}

{# customizable: defines whether to define program as active, annual, or both #}
{% set agg_type = var('edu:special_ed:active_or_annual', ['annual']) %}


with stage as (
    select * from {{ ref('stg_ef3__student_special_education_program_associations') }}
),

maxed as (
    -- take one row per student, maxing across kept rows
    select
        k_student,
        any_value(tenant_code) as tenant_code,

        {% if agg_type == 'active' or 'active' in agg_type %}
        max(
          {{ value_not_in_list(field='program_name', excluded_items=exclude_programs) }}
          and {{ start_date_column }} <= current_date() -- start date is today or in the past
          and ({{ exit_date_column }} is null -- no exit date
            or {{ exit_date_column }} > current_date()) -- exit date is in the future
        ) as is_special_education_active -- if the student has an active special education program enrollment
        {% endif %}

        {% if agg_type == 'annual' or 'annual' in agg_type %}
        max(
          {{ value_not_in_list(field='program_name', excluded_items=exclude_programs) }}
        ) as is_special_education_annual, -- the student had a special education program enrollment any time during the year
        {% endif %}

        max(is_idea_eligible) as is_idea_eligible,
        max(is_multiply_disabled) as is_multiply_disabled

    from stage

    group by 1
)

select * from maxed
