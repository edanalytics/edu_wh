-- depends_on: {{ ref('xwalk_assessment_score_values') }}
-- depends_on: {{ ref('xwalk_assessment_score_value_thresholds') }}
{{
  config(
    post_hook=[
        "alter table {{ this }} add primary key (k_student_assessment)",
        "alter table {{ this }} add constraint fk_{{ this.name }}_assessment foreign key (k_assessment) references {{ ref('dim_assessment') }}",
        "alter table {{ this }} add constraint fk_{{ this.name }}_student foreign key (k_student) references {{ ref('dim_student') }}"
    ]
  )
}}

with student_assessments_long_results as (
    select * from {{ ref('bld_ef3__student_assessments_long_results') }}
),
student_assessments as (
    select * from {{ ref('stg_ef3__student_assessments') }}
),
xwalk_assessment_seasons as (
    select * from {{ ref('xwalk_assessment_seasons') }}
),
object_agg_other_results as (
    select
        k_student_assessment,
        object_agg(original_score_name, score_result::variant) as v_other_results
    from student_assessments_long_results
    where normalized_score_name = 'other'
    group by 1
),
student_assessments_wide as (
    select
        student_assessments.k_student_assessment,
        student_assessments.k_assessment,
        student_assessments.k_student,
        student_assessments.tenant_code,
        student_assessments.student_assessment_identifier,
        student_assessments.serial_number,
        student_assessments.school_year,
        student_assessments.administration_date,
        student_assessments.administration_end_date,
        xwalk_assessment_seasons.season_name as administration_season,
        xwalk_assessment_seasons.season_num as administration_season_num,
        event_description,
        administration_environment,
        administration_language,
        event_circumstance,
        platform_type,
        reason_not_tested,
        retest_indicator,
        when_assessed_grade_level,
        v_other_results
        {%- if not is_empty_model('xwalk_assessment_scores') -%},
        {{ dbt_utils.pivot(
            'normalized_score_name',
            dbt_utils.get_column_values(ref('xwalk_assessment_scores'), 'normalized_score_name'),
            then_value='score_result',
            else_value='NULL',
            agg='max',
            quote_identifiers=False
        ) }},
        {#- find distinct score names that are in one of the normalize_result xwalks (distinct scores to add normalized_ column for) -#}
        {% set normalized_names_values = dbt_utils.get_column_values(ref('xwalk_assessment_score_values'), 'normalized_score_name') %}
        {% set normalized_names_thresholds = dbt_utils.get_column_values(ref('xwalk_assessment_score_value_thresholds'), 'normalized_score_name') or [] %}
        {{ dbt_utils.pivot(
            'normalized_score_name',
            (score_values_names + score_value_threshold_names) | unique,
            then_value='normalized_score_result',
            else_value='NULL',
            prefix='normalized_',
            agg='max',
            quote_identifiers=False
        ) }}
        {%- endif %}
    from student_assessments
    left join student_assessments_long_results
        on student_assessments.k_student_assessment = student_assessments_long_results.k_student_assessment
        and student_assessments_long_results.normalized_score_name != 'other'
    left join object_agg_other_results
        on student_assessments.k_student_assessment = object_agg_other_results.k_student_assessment
    left join xwalk_assessment_seasons
        on student_assessments.school_year = xwalk_assessment_seasons.school_year
        and student_assessments.administration_date >= xwalk_assessment_seasons.start_date
        and student_assessments.administration_date <= xwalk_assessment_seasons.end_date
    {{ dbt_utils.group_by(n=20) }}
)
select *
from student_assessments_wide
