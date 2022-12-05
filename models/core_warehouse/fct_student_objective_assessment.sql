{{
  config(
    post_hook=[
        "alter table {{ this }} add primary key (k_student_objective_assessment)",
        "alter table {{ this }} add constraint fk_{{ this.name }}_objective_assessment foreign key (k_objective_assessment) references {{ ref('dim_objective_assessment') }}",
        "alter table {{ this }} add constraint fk_{{ this.name }}_assessment foreign key (k_assessment) references {{ ref('dim_assessment') }}",
        "alter table {{ this }} add constraint fk_{{ this.name }}_student foreign key (k_student) references {{ ref('dim_student') }}"
    ]
  )
}}

with student_obj_assessments_long_results as (
    select * from {{ ref('bld_ef3__student_objective_assessments_long_results') }}
),
student_obj_assessments as (
    select * from {{ ref('stg_ef3__student_objective_assessments') }}
),
object_agg_other_results as (
    select
        k_student_objective_assessment,
        object_agg(original_score_name, score_result::variant) as v_other_results
    from student_obj_assessments_long_results
    where normalized_score_name = 'other'
    group by 1
),
student_obj_assessments_wide as (
    select
        student_obj_assessments.k_student_objective_assessment,
        student_obj_assessments.k_objective_assessment,
        student_obj_assessments.k_student_assessment,
        student_obj_assessments.k_assessment,
        student_obj_assessments.k_student,
        school_year,
        administration_date,
        administration_end_date,
        event_description,
        administration_environment,
        administration_language,
        event_circumstance,
        platform_type,
        reason_not_tested,
        retest_indicator,
        when_assessed_grade_level,
        v_other_results,
        {{ dbt_utils.pivot(
            'normalized_score_name',
            dbt_utils.get_column_values(ref('xwalk_objective_assessment_scores'), 'normalized_score_name'),
            then_value='score_result',
            else_value='NULL',
            agg='max',
            quote_identifiers=False
        ) }}
    from student_obj_assessments
    join student_obj_assessments_long_results
        on student_obj_assessments.k_student_objective_assessment = student_obj_assessments_long_results.k_student_objective_assessment
        and student_obj_assessments_long_results.normalized_score_name != 'other'
    left join object_agg_other_results
        on student_obj_assessments.k_student_objective_assessment = object_agg_other_results.k_student_objective_assessment
    group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17
)
select *
from student_obj_assessments_wide