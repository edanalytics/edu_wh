{# edu:assessment:score_names lists fct_student_assessment columns to include as score_name rows.
   edu:assessment:superscores adds computed superscore rows from objective assessment subscores. #}
{% set score_names = var('edu:assessment:score_names', ['scale_score', 'performance_level']) %}
{% set superscores = var('edu:assessment:superscores', {}) %}
{% set superscore_pairs = [] %}
{% for assessment_id, config in superscores.items() %}
    {% for code in config.subscore_codes %}
        {% do superscore_pairs.append("('" ~ assessment_id ~ "', '" ~ code ~ "')") %}
    {% endfor %}
{% endfor %}

with fct_student_assessment as (
    select * from {{ ref('fct_student_assessment') }}
),

dim_assessment as (
    select * from {{ ref('dim_assessment') }}
),

{% if superscores | length > 0 %}
fct_student_objective_assessment as (
    select * from {{ ref('fct_student_objective_assessment') }}
),

dim_objective_assessment as (
    select * from {{ ref('dim_objective_assessment') }}
),

-- single scan of fct_student_objective_assessment filtered by all configured (assessment, subscore) pairs
best_subscores as (

    select
        fct_student_objective_assessment.k_student_xyear,
        fct_student_objective_assessment.tenant_code,
        fct_student_objective_assessment.school_year,
        -- any administration's key, for lineage only; grouping by it would give one
        -- row per section per sitting, and the superscore would then sum or average
        -- every sitting's sections rather than each section's best
        max(fct_student_objective_assessment.k_assessment) as k_assessment,
        dim_assessment.assessment_identifier,
        dim_objective_assessment.objective_assessment_identification_code,
        max(try_to_double(fct_student_objective_assessment.scale_score)) as max_scale_score
    from fct_student_objective_assessment
    join dim_assessment
        on fct_student_objective_assessment.k_assessment = dim_assessment.k_assessment
    join dim_objective_assessment
        on fct_student_objective_assessment.k_objective_assessment = dim_objective_assessment.k_objective_assessment
    where (dim_assessment.assessment_identifier, dim_objective_assessment.objective_assessment_identification_code) in (
        {{ superscore_pairs | join(', ') }}
    )
    group by 1, 2, 3, 5, 6

),

{% endif %}

-- best score per student x year x assessment x score_name
annual_best as (

    {% for score_name in score_names %}
    select
        fct_student_assessment.k_student_xyear,
        fct_student_assessment.tenant_code,
        fct_student_assessment.school_year,
        dim_assessment.assessment_identifier,
        max_by(dim_assessment.k_assessment, try_to_double(fct_student_assessment.{{ score_name }})) as k_assessment,
        '{{ score_name }}' as score_name,
        max(try_to_double(fct_student_assessment.{{ score_name }}))::varchar as best_score
    from fct_student_assessment
    join dim_assessment
        on fct_student_assessment.k_assessment = dim_assessment.k_assessment
    group by 1, 2, 3, 4, score_name

    {% if not loop.last or superscores | length > 0 %}union all{% endif %}
    {% endfor %}

    {% for assessment_id, config in superscores.items() %}
    select
        k_student_xyear,
        tenant_code,
        school_year,
        assessment_identifier,
        max(k_assessment) as k_assessment,
        'superscore' as score_name,
        {% if config.method == 'avg_of_max' %}round(avg(max_scale_score))
        {% else %}sum(max_scale_score){% endif %}::varchar as best_score
    from best_subscores
    where assessment_identifier = '{{ assessment_id }}'
    group by 1, 2, 3, 4, score_name

    {% if not loop.last %}union all{% endif %}
    {% endfor %}

),

-- student x year x assessment x score_name rows from first year onward
-- so cumulative best scores propagate forward into years with no new scores
spine as (

    select sy.k_student_xyear, sy.tenant_code, sy.school_year, sd.assessment_identifier, sd.score_name
    from (
        select distinct k_student_xyear, tenant_code, school_year
        from fct_student_assessment
    ) as sy
    join (
        select k_student_xyear, tenant_code, assessment_identifier, score_name, min(school_year) as first_school_year
        from annual_best
        group by 1, 2, 3, 4
    ) as sd
        on  sy.k_student_xyear = sd.k_student_xyear
        and sy.tenant_code     = sd.tenant_code
        and sy.school_year    >= sd.first_school_year

),

-- cumulative best scores through each year
final as (

    select
        spine.k_student_xyear,
        spine.tenant_code,
        spine.school_year,
        spine.assessment_identifier,
        last_value(ab.k_assessment ignore nulls) over (
            partition by spine.k_student_xyear, spine.assessment_identifier, spine.score_name
            order by spine.school_year
            rows between unbounded preceding and current row
        ) as k_assessment,
        spine.score_name,
        -- compared as numbers: best_score is varchar, and a string max ranks '900'
        -- above '1600'. annual_best already reduced every score to a number or null
        max(try_to_double(ab.best_score)) over (
            partition by spine.k_student_xyear, spine.assessment_identifier, spine.score_name
            order by spine.school_year
            rows between unbounded preceding and current row
        )::varchar as best_score
    from spine
    left join annual_best as ab
        on  spine.k_student_xyear       = ab.k_student_xyear
        and spine.tenant_code           = ab.tenant_code
        and spine.school_year           = ab.school_year
        and spine.assessment_identifier = ab.assessment_identifier
        and spine.score_name            = ab.score_name

)

select * from final
