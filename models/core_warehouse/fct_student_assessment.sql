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
combined_with_cross_tenant as (
    select
        coalesce(student_assessment_cross_tenant.k_student_assessment, student_assessments.k_student_assessment) as k_student_assessment,
        coalesce(student_assessment_cross_tenant.k_assessment, student_assessments.k_assessment) as k_assessment,
        coalesce(student_assessment_cross_tenant.k_student, student_assessments.k_student) as k_student,
        coalesce(student_assessment_cross_tenant.k_student_xyear, student_assessments.k_student_xyear) as k_student_xyear,
        coalesce(student_assessment_cross_tenant.tenant_code, student_assessments.tenant_code) as tenant_code,
        coalesce(student_assessment_cross_tenant.school_year, student_assessments.school_year) as school_year,
        coalesce(student_assessment_cross_tenant.k_student_assessment__original, student_assessments.k_student_assessment) as k_student_assessment__original,
        coalesce(student_assessment_cross_tenant.is_original_record, True) as is_original_record,
        coalesce(student_assessment_cross_tenant.original_tenant_code, student_assessments.tenant_code) as original_tenant_code,
        {{ accordion_columns(
            source_table='stg_ef3__student_assessments',
            source_alias='student_assessments',
            exclude_columns=['k_student_assessment', 'k_assessment', 'k_student', 'k_student_xyear', 'tenant_code', 'school_year'],
            add_trailing_comma=false) }}
    from student_assessments
    -- left join because this model can return empty
        -- and to avoid enforcing a current school association
    left join student_assessment_cross_tenant
        on student_assessments.k_student_assessment = student_assessment_cross_tenant.k_student_assessment__original
),
student_assessments_wide as (
    select
        stu_xtenant.k_student_assessment,
        stu_xtenant.k_student_assessment__original,
        stu_xtenant.k_assessment,
        -- use dim_student.k_student. NOTE, will be null when no corresponding demographics found (e.g. historic year of assessment data)
        dim_student.k_student,
        stu_xtenant.k_student_xyear,
        stu_xtenant.tenant_code,
        stu_xtenant.is_original_record,
        stu_xtenant.original_tenant_code,
        stu_xtenant.student_assessment_identifier,
        stu_xtenant.serial_number,
        stu_xtenant.school_year,
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
        {%- endif %}
        {# add any extension columns configured from stg_ef3__student_assessments #}
        {{ edu_edfi_source.extract_extension(model_name='stg_ef3__student_assessments', flatten=False) }}
    from combined_with_cross_tenant as stu_xtenant
    left join student_assessments_long_results
        on stu_xtenant.k_student_assessment__original = student_assessments_long_results.k_student_assessment
        and student_assessments_long_results.normalized_score_name != 'other'
    -- left join to allow 'historic' records (assess records with no corresponding stu demographics)
    left join dim_student
        on stu_xtenant.k_student = dim_student.k_student
    -- FILTER to students who EVER have a record in dim_student
    where stu_xtenant.k_student_xyear in (
        select distinct k_student_xyear
        from dim_student
    )
    {{ dbt_utils.group_by(n=21) }}
)
-- add v_other_results to the end because variant columns cannot be included in a group by in Databricks
select
    {{ edu_edfi_source.star('student_assessments_wide', except=([] if var('edu:assessment:cross_tenant_enabled', False) else ['k_student_assessment__original', 'is_original_record', 'original_tenant_code'])) }},
    v_other_results
from student_assessments_wide
left join object_agg_other_results
    on student_assessments_wide.k_student_assessment__original = object_agg_other_results.k_student_assessment
