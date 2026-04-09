{% if var("edu:assessment:cross_tenant_enabled", False) -%}

with student_obj_assessments as (
    select * from {{ ref('stg_ef3__student_objective_assessments') }}
),
student_assessment_cross_tenant as (
    select * from {{ ref('bld_ef3__student_assessment_cross_tenant') }}
),
-- NOTE: the k_objective_assessment formula here must stay in sync with
-- the cross-tenant branch in dim_objective_assessment.
-- a divergence will produce FK orphans.
cross_tenant_keys as (
    select
        {{ dbt_utils.generate_surrogate_key(
            ['student_assessment_cross_tenant.tenant_code',
            'student_assessment_cross_tenant.api_year',
            'lower(student_obj_assessments.assess_academic_subject)',
            'lower(student_obj_assessments.assessment_identifier)',
            'lower(student_obj_assessments.namespace)',
            'lower(student_obj_assessments.objective_assessment_identification_code)',
            'lower(student_obj_assessments.student_assessment_identifier)']
        ) }} as k_student_objective_assessment,
        {{ dbt_utils.generate_surrogate_key(
            ['student_assessment_cross_tenant.tenant_code',
            'student_assessment_cross_tenant.api_year',
            'lower(student_obj_assessments.assess_academic_subject)',
            'lower(student_obj_assessments.academic_subject)',
            'lower(student_obj_assessments.assessment_identifier)',
            'lower(student_obj_assessments.namespace)',
            'lower(student_obj_assessments.objective_assessment_identification_code)'
            ]
        ) }} as k_objective_assessment,
        student_obj_assessments.k_student_objective_assessment as k_student_objective_assessment__original,
        student_obj_assessments.k_objective_assessment                as k_objective_assessment__original,
        student_assessment_cross_tenant.k_student_assessment,
        student_assessment_cross_tenant.k_student_assessment__original,
        student_assessment_cross_tenant.k_student,
        student_assessment_cross_tenant.k_student_xyear,
        student_assessment_cross_tenant.k_assessment,
        student_assessment_cross_tenant.k_assessment__original,
        student_assessment_cross_tenant.tenant_code,
        student_assessment_cross_tenant.school_year,
        student_assessment_cross_tenant.api_year,
        student_assessment_cross_tenant.is_original_record,
        student_assessment_cross_tenant.original_tenant_code
    from student_obj_assessments
    join student_assessment_cross_tenant
        on student_obj_assessments.k_student_assessment = student_assessment_cross_tenant.k_student_assessment__original
),
deduped as (
    {{
        dbt_utils.deduplicate(
            relation='cross_tenant_keys',
            partition_by='k_student_objective_assessment',
            order_by='is_original_record desc'
        )
    }}
)
select * from deduped

{% else %}

select * from (
    select
        null::{{ dbt.type_string() }} as k_student_objective_assessment,
        null::{{ dbt.type_string() }} as k_objective_assessment,
        null::{{ dbt.type_string() }} as k_student_objective_assessment__original,
        null::{{ dbt.type_string() }} as k_objective_assessment__original,
        null::{{ dbt.type_string() }} as k_student_assessment,
        null::{{ dbt.type_string() }} as k_student_assessment__original,
        null::{{ dbt.type_string() }} as k_student,
        null::{{ dbt.type_string() }} as k_student_xyear,
        null::{{ dbt.type_string() }} as k_assessment,
        null::{{ dbt.type_string() }} as k_assessment__original,
        null::{{ dbt.type_string() }} as tenant_code,
        null::int as school_year,
        null::int as api_year,
        null::boolean as is_original_record,
        null::{{ dbt.type_string() }} as original_tenant_code
) limit 0

{% endif %}
