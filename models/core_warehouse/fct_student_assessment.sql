{{
  config(
    post_hook=[
        "alter table {{ this }} alter column k_student_assessment set not null",
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
student_assessment_cross_tenant as (
    select * from {{ ref('bld_ef3__student_assessment_cross_tenant') }}
),
dim_student as (
    select * from {{ ref('dim_student') }}
),
object_agg_other_results as (
    select
        k_student_assessment,
        {{ edu_edfi_source.json_object_agg('original_score_name', 'score_result') }} as v_other_results
    from student_assessments_long_results
    where normalized_score_name = 'other'
    group by 1
),
access as (
    select
        coalesce(student_assessment_cross_tenant.k_student_assessment, student_assessments.k_student_assessment) as k_student_assessment,
        coalesce(student_assessment_cross_tenant.k_assessment, student_assessments.k_assessment) as k_assessment,
        coalesce(student_assessment_cross_tenant.k_student, student_assessments.k_student) as k_student,
        coalesce(student_assessment_cross_tenant.k_student_xyear, student_assessments.k_student_xyear) as k_student_xyear,
        coalesce(student_assessment_cross_tenant.tenant_code, student_assessments.tenant_code) as tenant_code,
        coalesce(student_assessment_cross_tenant.school_year, student_assessments.school_year) as school_year,
        coalesce(student_assessment_cross_tenant.k_student_assessment__original, student_assessments.k_student_assessment) as k_student_assessment__original,
        {{ accordion_columns(
            source_table='stg_ef3__student_assessments', 
            source_alias='student_assessments',
            exclude_columns=['k_student_assessment', 'k_assessment', 'k_student', 'k_student_xyear', 'tenant_code', 'school_year']) }}
    from student_assessments
    -- left join because this model can return empty
        -- and to avoid enforcing a current school association
    left join student_assessment_cross_tenant
        on student_assessments.k_student_assessment = student_assessment_cross_tenant.k_student_assessment__original
),
student_assessments_wide as (
    select
        access.k_student_assessment,
        access.k_student_assessment__original,
        access.k_assessment,
        -- use dim_student.k_student. NOTE, will be null when no corresponding demographics found (e.g. historic year of assessment data)
        dim_student.k_student,
        access.k_student_xyear,
        access.tenant_code,
        access.student_assessment_identifier,
        access.serial_number,
        {% if var('edu:school_year:assessment_dates_xwalk_enabled', False) %}
        iff(dates_xwalk.override_existing,
            coalesce(dates_xwalk.school_year, access.school_year, {{derive_school_year('access.administration_date')}}),
            coalesce(access.school_year, dates_xwalk.school_year, {{derive_school_year('access.administration_date')}}))
        as school_year,
        {% else %}
        coalesce(access.school_year, {{derive_school_year('access.administration_date')}}) as school_year,
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
        {%- if not is_empty_model('xwalk_assessment_scores') -%},
        {{ dbt_utils.pivot(
            'normalized_score_name',
            dbt_utils.get_column_values(ref('xwalk_assessment_scores'), 'normalized_score_name'),
            then_value='score_result',
            else_value='NULL',
            agg='max',
            quote_identifiers=False
        ) }}
        {%- endif %},
        is_original_record,
        original_tenant_code,
        {# add any extension columns configured from stg_ef3__student_assessments #}
        {{ edu_edfi_source.extract_extension(model_name='stg_ef3__student_assessments', flatten=False) }}
    from access
    left join student_assessments_long_results
        on access.k_student_assessment__original = student_assessments_long_results.k_student_assessment
        and student_assessments_long_results.normalized_score_name != 'other'
    -- left join to allow 'historic' records (assess records with no corresponding stu demographics)
    left join dim_student
        on access.k_student = dim_student.k_student
    {% if var('edu:school_year:assessment_dates_xwalk_enabled', False) %}
    left join {{ ref('xwalk_assessment_school_year_dates') }} dates_xwalk
        -- note: between means A >= X AND A <= Y, so date upper/lower bounds should not overlap across years
        on access.administration_date between dates_xwalk.start_date::date and dates_xwalk.end_date::date 
        -- we want to allow for the school year cutoffs to differ by assessment 
        -- but also allow those fields to remain null if xwalk is desired but not to differ across assessments
        and ifnull(dates_xwalk.assessment_identifier, '1') = iff(dates_xwalk.assessment_identifier is null, '1', access.assessment_identifier)
        and ifnull(dates_xwalk.namespace, '1') = iff(dates_xwalk.namespace is null, '1', access.namespace)
    {% endif %}
    -- FILTER to students who EVER have a record in dim_student
    where access.k_student_xyear in (
        select distinct k_student_xyear
        from dim_student
    )
    {{ dbt_utils.group_by(n=19) }}
)
-- add v_other_results to the end because variant columns cannot be included in a group by in Databricks
select 
    {{ star('student_assessments_wide', except=['k_student_assessment__original']) }},, 
    v_other_results
from student_assessments_wide
left join object_agg_other_results
    -- TODO: I don't want this column to be part of the output
    on student_assessments_wide.k_student_assessment__original = object_agg_other_results.k_student_assessment
