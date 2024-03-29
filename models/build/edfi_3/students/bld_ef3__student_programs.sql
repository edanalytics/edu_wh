-- Creates a list of program types from the xwalk
{% set program_types_query %}
    select 
        indicator_name, 
        is_active, 
        is_annual, 
        is_ever 
    from {{ ref('xwalk_student_programs')}}
    group by indicator_name, is_active, is_annual, is_ever 
    order by 1
{% endset %}

{% if execute %}
{% set results = run_query(program_types_query) %}
{% set program_types = results.columns[0].values() %} 
{% set include_is_active = results.columns[1].values() %} 
{% set include_is_annual = results.columns[2].values() %} 
{% set include_is_ever = results.columns[3].values() %} 
{% else %}
{% set program_types = [] %}
{% endif %}

with stage as (
    select * from {{ ref('stg_ef3__student_program_associations')}}
),

program_xwalk as (
    select * from {{ ref('xwalk_student_programs')}}
),

maxed as (
    select
        k_student,
        k_student_xyear,
        any_value(tenant_code) as tenant_code,
        {% for program_type in program_types %}
            max(
                program_xwalk.indicator_name = '{{ program_type }}'
                and program_enroll_begin_date <= current_date() -- start date is today or in the past
                and (program_enroll_end_date is null -- no exit date
                    or program_enroll_end_date > current_date()) -- exit date is in the future
            ) as is_{{ program_type }}_active, -- the student has an active program enrollment
            
            max(
                program_xwalk.indicator_name = '{{ program_type }}'
            ) as is_{{ program_type }}_annual, -- the student had a program enrollment any time during the year
        {% endfor%}
        any_value(ed_org_id) as ed_org_id -- placed at the end to avoid comma issues with the loop
    from stage
    join program_xwalk
        on stage.program_name = program_xwalk.program_name
    group by 1, 2
),

xyear_agged as (
    select
        k_student_xyear,
        {% for program_type in program_types %}
            max(is_{{ program_type }}_annual) as is_{{ program_type }}_ever,
        {% endfor%}
        any_value(k_student) -- placed at the end to avoid comma issues with the loop
    from maxed
    group by 1
),

joined as (
    select
        maxed.k_student,
        maxed.k_student_xyear,
        maxed.tenant_code,
        {% for program_type in program_types %}
            {% if include_is_active[loop.index0] == true %}
                maxed.is_{{ program_type }}_active,
            {% endif %}

            {% if include_is_annual[loop.index0] == true %}
                is_{{ program_type }}_annual,
            {% endif %}

            {% if include_is_ever[loop.index0] == true %}
                xyear_agged.is_{{ program_type }}_ever,
            {% endif %}
        {% endfor%}
        maxed.ed_org_id -- placed at the end to avoid comma issues with the loop
    from maxed
        left join xyear_agged
        on maxed.k_student_xyear = xyear_agged.k_student_xyear
)

select * from joined