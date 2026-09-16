{% set gpa_type = var('edu:gpa:gpa_type', none) %}
{% set academic_terms = var('edu:gpa:academic_terms', []) %}
{% set cumulative_only = var('edu:gpa:cumulative_only', true) %}

with fct_student_gpa as (
    select * from {{ ref('fct_student_gpa') }}
),

cumulative_gpa as (

    select
        fct_student_gpa.k_student_xyear,
        fct_student_gpa.tenant_code,
        fct_student_gpa.school_year,
        max(fct_student_gpa.gpa_value) as gpa,
        -- scale ceiling; stable within a gpa_type so any_value is safe
        any_value(fct_student_gpa.max_gpa_value) as max_gpa_value

    from fct_student_gpa

    where true

    {# This is optional, partly bc the is_cumulative flag may be null depending on the version of Ed-Fi that sourced the data #}
    {% if cumulative_only %}
    and fct_student_gpa.is_cumulative
    {% endif %}

    {% if gpa_type is not none %}
    and fct_student_gpa.gpa_type = '{{ gpa_type }}'
    {% endif %}

    {% if academic_terms | length > 0 %}
    and fct_student_gpa.academic_term in ('{{ academic_terms | join("', '") }}')
    {% endif %}

    group by 1, 2, 3

)

select * from cumulative_gpa
