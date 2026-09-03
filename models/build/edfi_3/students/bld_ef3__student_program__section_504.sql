{# customizable: certain program names may be excluded and not counted as section 504 #}
{% set exclude_programs = var('edu:section_504:exclude_programs') %}

{# customizable: the column that defines the start date for the section 504 programs #}
{% set start_date_column = var('edu:section_504:start_date_column') %}

{# customizable: the column that defines the end date for the section 504 programs #}
{% set exit_date_column = var('edu:section_504:exit_date_column') %}

{# customizable: extra indicators to create in the aggregate query #}
{% set custom_program_agg_indicators = var('edu:section_504:custom_program_agg_indicators', None) %}

with stage as (
    select * from {{ ref('stg_ef3__student_section_504_program_associations') }}
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
        ) as is_section_504_active,

        max(
          {{ value_not_in_list(field='program_name', excluded_items=exclude_programs) }}
        ) as is_section_504_annual,

        -- custom section 504 program agg indicators
        {% if custom_program_agg_indicators -%}
          {%- for indicator in custom_program_agg_indicators -%}
            {{ custom_program_agg_indicators[indicator]['agg_sql'] }} as {{ indicator }},
          {%- endfor -%}
        {%- endif %}

        max(accommodation_plan) as accommodation_plan,
        max(section_504_eligibility) as section_504_eligibility,
        max(section_504_eligibility_decision_date) as section_504_eligibility_decision_date,
        max(section_504_meeting_date) as section_504_meeting_date,
        max(section_504_disability) as section_504_disability

    from stage
    group by 1, 2
),

xyear_agged as (
    select
        k_student_xyear,
        max(is_section_504_annual) as is_section_504_ever

    from maxed
    group by 1
),

joined as (
    select
        maxed.*,
        xyear_agged.is_section_504_ever

    from maxed
        left join xyear_agged
        on maxed.k_student_xyear = xyear_agged.k_student_xyear
)

select * from joined
