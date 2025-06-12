{{
  config(
    post_hook=[
        "alter table {{ this }} alter column k_student_objective_assessment set not null",
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
dim_student as (
    select * from {{ ref('dim_student') }}
),
object_agg_other_results as (
    select
        k_student_objective_assessment,
        {{ edu_edfi_source.json_object_agg('original_score_name', 'score_result') }} as v_other_results
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
        dim_student.k_student,
        student_obj_assessments.k_student_xyear,
        student_obj_assessments.tenant_code,
        {% if var('edu:school_year:assessment_dates_xwalk_enabled', False) %}
        iff(dates_xwalk.override_existing,
            coalesce(dates_xwalk.school_year, student_obj_assessments.school_year, {{derive_school_year('student_obj_assessments.administration_date')}}),
            coalesce(student_obj_assessments.school_year, dates_xwalk.school_year, {{derive_school_year('student_obj_assessments.administration_date')}}))
        as school_year,
        {% else %}
        coalesce(student_obj_assessments.school_year, {{derive_school_year('student_obj_assessments.administration_date')}}) as school_year,
        {% endif %}
        administration_date,
        administration_end_date,
        event_description,
        administration_environment,
        administration_language,
        event_circumstance,
        platform_type,
        reason_not_tested,
        retest_indicator,
        when_assessed_grade_level
        {%- if not is_empty_model('xwalk_objective_assessment_scores') -%},
        {{ dbt_utils.pivot(
            'normalized_score_name',
            dbt_utils.get_column_values(ref('xwalk_objective_assessment_scores'), 'normalized_score_name'),
            then_value='score_result',
            else_value='NULL',
            agg='max',
            quote_identifiers=False
        ) }}
        {%- endif %}
        {# add any extension columns configured from stg_ef3__student_objective_assessments #}
        {{ edu_edfi_source.extract_extension(model_name='stg_ef3__student_objective_assessments', flatten=False) }}
    from student_obj_assessments
    left join student_obj_assessments_long_results
        on student_obj_assessments.k_student_objective_assessment = student_obj_assessments_long_results.k_student_objective_assessment
        and student_obj_assessments_long_results.normalized_score_name != 'other'
    -- left join to allow 'historic' records (assess records with no corresponding stu demographics)
    left join dim_student
        on student_obj_assessments.k_student = dim_student.k_student
    {% if var('edu:school_year:assessment_dates_xwalk_enabled', False) %}
    left join {{ ref('xwalk_assessment_school_year_dates') }} dates_xwalk
        -- note: between means A >= X AND A <= Y, so date upper/lower bounds should not overlap across years
        on student_obj_assessments.administration_date between start_date::date and end_date::date
        -- we want to allow for the school year cutoffs to differ by assessment 
        -- but also allow those fields to remain null if xwalk is desired but not to differ across assessments
        and ifnull(dates_xwalk.assessment_identifier, '1') = iff(dates_xwalk.assessment_identifier is null, '1', student_obj_assessments.assessment_identifier)
        and ifnull(dates_xwalk.namespace, '1') = iff(dates_xwalk.namespace is null, '1', student_obj_assessments.namespace)
    {% endif %}
    -- FILTER to students who EVER have a record in dim_student
    where student_obj_assessments.k_student_xyear in (
        select distinct k_student_xyear
        from dim_student
    )
    {{ dbt_utils.group_by(n=18) }}
)
-- add v_other_results to the end because variant columns cannot be included in a group by in Databricks
select 
    student_obj_assessments_wide.*,
    v_other_results
from student_obj_assessments_wide
left join object_agg_other_results
    on student_obj_assessments_wide.k_student_objective_assessment = object_agg_other_results.k_student_objective_assessment
