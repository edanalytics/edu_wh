{# customizable: certain program names may be excluded and not counted as neglected or delinquent #}
{% set exclude_programs = var('edu:neglected_or_delinquent:exclude_programs') %}

{# customizable: the column that defines the start date for the neglected or delinquent programs #}
{% set start_date_column = var('edu:neglected_or_delinquent:start_date_column') %}

{# customizable: the column that defines the end date for the neglected or delinquent programs #}
{% set exit_date_column = var('edu:neglected_or_delinquent:exit_date_column') %}

{# customizable: extra indicators to create in the aggregate query #}
{% set custom_program_agg_indicators = var('edu:neglected_or_delinquent:custom_program_agg_indicators', None) %}

with stage as (
    select * from {{ ref('stg_ef3__student_neglected_or_delinquent_program_associations') }}
),

maxed as (
    -- take one row per student, maxing across kept rows
    select
        k_student,
        k_student_xyear,
        any_value(tenant_code) as tenant_code,

        max(
          {{ value_not_in_list(field='program_name', excluded_items=exclude_programs) }}
          and {{ start_date_column }} <= current_date()
          and ({{ exit_date_column }} is null or {{ exit_date_column }} > current_date())
        ) as is_neglected_or_delinquent_active,

        max(
          {{ value_not_in_list(field='program_name', excluded_items=exclude_programs) }}
        ) as is_neglected_or_delinquent_annual,

        -- custom neglected/delinquent program agg indicators
        {% if custom_program_agg_indicators -%}
          {%- for indicator in custom_program_agg_indicators -%}
            {{ custom_program_agg_indicators[indicator]['agg_sql'] }} as {{ indicator }},
          {%- endfor -%}
        {%- endif %}

        max(served_outside_of_regular_session) as served_outside_of_regular_session,
        max(ela_progress_level) as ela_progress_level,
        max(mathematics_progress_level) as mathematics_progress_level,
        max(neglected_or_delinquent_program) as neglected_or_delinquent_program

    from stage
    group by 1, 2
),

xyear_agged as (
    select
        k_student_xyear,
        max(is_neglected_or_delinquent_annual) as is_neglected_or_delinquent_ever

    from maxed
    group by 1
),

joined as (
    select
        maxed.*,
        xyear_agged.is_neglected_or_delinquent_ever

    from maxed
        left join xyear_agged
        on maxed.k_student_xyear = xyear_agged.k_student_xyear
)

select * from joined
