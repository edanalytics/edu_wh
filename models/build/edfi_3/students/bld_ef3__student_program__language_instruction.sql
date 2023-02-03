{# customizable: certain program names may be excluded and not counted as language instruction #}
{% set exclude_programs = var('edu:language_instruction:exclude_programs') %}

{# customizable: the column that defines the start date for the language instruction program #}
{% set start_date_column = var('edu:language_instruction:start_date_column') %}

{# customizable: the column that defines the end date for the language instruction program #}
{% set exit_date_column = var('edu:language_instruction:exit_date_column') %}


with stage as (
    select * from {{ ref('stg_ef3__student_language_instruction_program_associations') }}
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
        ) as is_english_language_learner_active, -- if the student has an active language instruction program enrollment

        max(
          {{ value_not_in_list(field='program_name', excluded_items=exclude_programs) }}
        ) as is_english_language_learner_annual, -- the student had a language instruction program enrollment any time during the year

        max(has_english_learner_participation) as has_english_learner_participation

    from stage

    group by 1, 2
),

xyear_agged as (
    select
        k_student_xyear,
        boolor_agg(is_english_language_learner_annual) as is_english_language_learner_ever

    from maxed
    group by 1
),

joined as (
    select
        maxed.*,
        xyear_agged.is_english_language_learner_ever

    from maxed
        left join xyear_agged
        on maxed.k_student_xyear = xyear_agged.k_student_xyear
)

select * from joined
