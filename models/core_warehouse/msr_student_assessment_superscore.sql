{% set superscores = var('edu:assessment:superscores', {}) %}

with objective_scores as (
    select * from {{ ref('msr_student_cumulative_objective_assessment_score') }}
),

{% if superscores | length > 0 %}

superscores as (

    {% for assessment_id, config in superscores.items() %}

    select
        objective_scores.k_student_xyear,
        objective_scores.tenant_code,
        objective_scores.school_year,
        '{{ assessment_id }}' as assessment_identifier,
        sum(objective_scores.scale_score) as superscore

    from objective_scores

    where objective_scores.assessment_identifier = '{{ assessment_id }}'
        and objective_scores.objective_assessment_identification_code
            in ('{{ config.subscore_codes | join("', '") }}')

    group by 1, 2, 3, 4

    {% if not loop.last %}union all{% endif %}

    {% endfor %}

)

select * from superscores

{% else %}

-- no superscores configured; returns empty table
select
    null::varchar as k_student_xyear,
    null::varchar as tenant_code,
    null::int    as school_year,
    null::varchar as assessment_identifier,
    null::float  as superscore
where false

{% endif %}
