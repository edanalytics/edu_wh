{# customizable: certain program names may be excluded and not counted as title_i #}
{% set exclude_programs = var('edu:title_i:exclude_programs', []) %}

{# customizable: the column that defines the start date for the title_i program #}
{% set start_date_column = var('edu:title_i:start_date_column', 'program_enroll_begin_date') %}

{# customizable: the column that defines the end date for the title_i program #}
{% set exit_date_column = var('edu:title_i:exit_date_column', 'program_enroll_end_date') %}

{# customizable: defines whether to define program as active, annual, or both #}
{% set agg_type = var('edu:title_i:active_or_annual', ['annual', 'active']) %}


with stage as (
    select * from {{ ref('stg_ef3__student_title_i_part_a_program_associations') }}
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
        ) as is_title_i_active, -- if the student has an active title_i program enrollment
        {% endif %}

        {% if agg_type == 'annual' or 'annual' in agg_type %}
        max(
          {{ value_not_in_list(field='program_name', excluded_items=exclude_programs) }}
        ) as is_title_i_annual, -- the student had a title_i program enrollment any time during the year
        {% endif %}

        max(title_i_part_a_participant_status) as title_i_part_a_participant_status

    from stage

    group by 1
)

select * from maxed