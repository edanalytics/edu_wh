{# customizable: certain program names may be excluded and not counted as homeless #}
{% set exclude_programs = var('edu:migrant_education:exclude_programs') %}

{# customizable: the column that defines the start date for the homeless program #}
{% set start_date_column = var('edu:migrant_education:start_date_column') %}

{# customizable: the column that defines the end date for the homeless program #}
{% set exit_date_column = var('edu:migrant_education:exit_date_column') %}

{# customizable: extra indicators to create in the aggregate query #}
{% set custom_program_agg_indicators = var('edu:migrant_education:custom_program_agg_indicators', None) %}

with stage as (
    select * from {{ ref('stg_ef3__student_migrant_education_program_associations') }}
),

maxed as (
    -- take one row per student, maxing across kept rows
    select 
        k_student,
        k_student_xyear,
        any_value(tenant_code) as tenant_code,
    
        max(
          {{ value_not_in_list(field='program_name', excluded_items=exclude_programs) }}
          and {{ start_date_column }} <= current_date() -- start date is today or in the past
          and ({{ exit_date_column }} is null -- no exit date
            or {{ exit_date_column }} > current_date()) -- exit date is in the future
        ) as is_migrant_education_active, -- if the student has an active migrant education program enrollment

        max(
          {{ value_not_in_list(field='program_name', excluded_items=exclude_programs) }}
        ) as is_migrant_education_annual, -- the student had a migrant education program enrollment any time during the year

        -- custom migrant education program agg indicators
        {% if custom_program_agg_indicators -%}
          {%- for indicator in custom_program_agg_indicators -%}
            {{ custom_program_agg_indicators[indicator]['agg_sql'] }} as {{ indicator }},
          {%- endfor -%}
        {%- endif %}

        max(priority_for_service) as priority_for_service,
        max(continuation_of_services_reason) as continuation_of_services_reason, 


    from stage
    group by 1, 2
),

xyear_agged as (
    select
        k_student_xyear,
        max(is_migrant_education_annual) as is_migrant_education_ever

    from maxed
    group by 1
),

joined as (
    select
        maxed.*,
        xyear_agged.is_migrant_education_ever

    from maxed
        left join xyear_agged
        on maxed.k_student_xyear = xyear_agged.k_student_xyear
)

select * from joined