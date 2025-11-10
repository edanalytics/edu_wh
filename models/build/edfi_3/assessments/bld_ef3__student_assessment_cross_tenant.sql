{% if var("edu:assessments:assessment_cross_tenant", False) -%}

{# Load students to remove source from var #}
{% set removed_students_source = var("edu:assessments:removed_students_source") %}

with dim_student as (
    select * from {{ ref('dim_student') }}
),
stg_student_assessment as (
    select * from {{ ref('stg_ef3__student_assessments') }}
),
fct_student_school as (
    select * from {{ ref('fct_student_school_association') }}
),
-- goal of this model is to determine which tenants have active enrollments
-- this will contain duplicates because the grain is of a school enrollment
active_enrollments as (
    select 
        fct_student_school.school_year,
        fct_student_school.tenant_code,
        fct_student_school.k_student,
        fct_student_school.k_student_xyear,
        dim_student.student_unique_id,
        dim_student.birth_date,
        dim_student.first_name,
        dim_student.last_name
    from fct_student_school
    join dim_student
        on fct_student_school.k_student = dim_student.k_student
    -- if this source is configured, remove these students instead of using default logic below
    {% if removed_students_source is not none and removed_students_source | length -%}
    left join {{ ref(removed_students_source) }}
        on dim_student.k_student = {{ removed_students_source }}.k_student
    where is_active_enrollment
    and {{ removed_students_source }}.k_student is not null
    {% else %}
    where is_active_enrollment
    -- below is default logic to ensure the chosen 'global' IDs actually represent the same student
    -- only to be used if a students to remove source is not configured
    qualify 
        -- either the birthdates are the same
        (count(distinct dim_student.birth_date) over (partition by dim_student.student_unique_id) <= 1
            -- at least 1 of birthdate, first name, or last name must be the same
            or (
                count(distinct dim_student.birth_date) over (partition by dim_student.student_unique_id)
                + count(distinct lower(dim_student.first_name)) over (partition by dim_student.student_unique_id)
                + count(distinct lower(dim_student.last_name))  over (partition by dim_student.student_unique_id)
            ) <= 5)
        and count(distinct fct_student_school.tenant_code) over(partition by dim_student.student_unique_id) > 1
    {% endif %}
),
-- we need to use school enrollments to look at start and end dates, but we want the grain to be unique by student, year, and tenant
deduped_enrollments as (
    {{
        dbt_utils.deduplicate(
            relation='active_enrollments',
            partition_by='school_year,tenant_code,k_student',
            order_by='school_year,tenant_code,k_student'
        )
    }}
),
subset_assessments as (
    select
        -- bring in the tenant code and student surrogate keys from enrollments
        deduped_enrollments.tenant_code,
        deduped_enrollments.school_year,
        deduped_enrollments.k_student,
        deduped_enrollments.k_student_xyear,
        -- recreate the surrogate keys with new tenant code
        -- TODO: this key may not join to anything
            -- but, can't leave original because of RLS
            -- create new records? complicated logic
        {{dbt_utils.generate_surrogate_key(
            ['deduped_enrollments.tenant_code',
            'deduped_enrollments.school_year',
            'lower(stg_student_assessment.academic_subject)',
            'lower(stg_student_assessment.assessment_identifier)',
            'lower(stg_student_assessment.namespace)']
        ) }} as k_assessment,
        {{ dbt_utils.generate_surrogate_key(
            ['deduped_enrollments.tenant_code',
            'deduped_enrollments.school_year',
            'lower(stg_student_assessment.academic_subject)',
            'lower(stg_student_assessment.assessment_identifier)',
            'lower(stg_student_assessment.namespace)',
            'lower(stg_student_assessment.student_assessment_identifier)',
            'lower(stg_student_assessment.student_unique_id)']
        ) }} as k_student_assessment,
        -- keep the original k_student_assessment for merging downstream
        stg_student_assessment.k_student_assessment as k_student_assessment__original,
        stg_student_assessment.k_assessment as k_assessment__original,
        stg_student_assessment.student_unique_id,
        case
            when stg_student_assessment.tenant_code = deduped_enrollments.tenant_code
                then 1
            else 0
        end as is_original_record,
        case
            when stg_student_assessment.tenant_code = deduped_enrollments.tenant_code
                then null
            else stg_student_assessment.tenant_code
        end as original_tenant_code
    from stg_student_assessment
    -- this code will intentionally create dupes to associate a student assessment record
    -- to every tenant where a current enrollment exists for that student_unique_id

    -- TODO: inner join or left join here?
    -- inner join would enforce at least 1 current SCHOOL enrollment within a tenant
    -- which we don't currently enforce
    join deduped_enrollments
        -- TODO: how to ensure this to be globally unique?
            -- only enforced uniqueness is by partner/yr
            -- test or bake into code somehow?
                -- qualify 1 = count(distinct bday) by student_unique_id

        -- NOTE: in the future, this could be configurable
        on stg_student_assessment.student_unique_id = deduped_enrollments.student_unique_id
)
select *
from subset_assessments
{% else %}
  -- if this feature is not turned on, force return a zero row table
  select *
  from (select
          null as tenant_code,
          null as school_year,
          null as k_student,
          null as k_student_xyear,
          null as k_assessment,
          null as k_student_assessment,
          null as k_student_assessment__original,
          null as student_unique_id,
          null as is_original_record
       ) blank_subquery
  limit 0
{% endif %}