{# customizable: certain program names may be excluded and not counted as homeless #}
{% set exclude_programs = var('edu:homeless:exclude_programs') %}

{# customizable: the column that defines the start date for the homeless program #}
{% set start_date_column = var('edu:homeless:start_date_column') %}

{# customizable: the column that defines the end date for the homeless program #}
{% set exit_date_column = var('edu:homeless:exit_date_column') %}


with stage as (
    select * from {{ ref('stg_ef3__student_homeless_program_associations') }}
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
        ) as is_homeless_active, -- if the student has an active homeless program enrollment

        max(
          {{ value_not_in_list(field='program_name', excluded_items=exclude_programs) }}
        ) as is_homeless_annual, -- the student had a homeless program enrollment any time during the year

        max(is_awaiting_foster_care) as is_awaiting_foster_care,
        max(is_homeless_unaccompanied_youth) as is_homeless_unaccompanied_youth,
        max(homeless_primary_nighttime_residence) as homeless_primary_nighttime_residence

    from stage
    group by 1, 2
),

xyear_agged as (
    select
        k_student_xyear,
        max(is_homeless_annual) as is_homeless_ever

    from maxed
    group by 1
),

joined as (
    select
        maxed.*,
        xyear_agged.is_homeless_ever

    from maxed
        left join xyear_agged
        on maxed.k_student_xyear = xyear_agged.k_student_xyear
)

select * from joined
