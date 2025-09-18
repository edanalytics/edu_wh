{% set custom_data_sources_name = "edu:student_subgroup:custom_data_sources" %}

with dim_student as (
    select * from {{ ref('dim_student') }}
),
dim_subgroup as (
    select * from {{ ref('dim_subgroup') }}
),

{% set stu_id_cols = dbt_utils.get_filtered_columns_in_relation(
        ref('bld_ef3__wide_ids_student'),
        except=['tenant_code', 'api_year', 'k_student', 'k_student_xyear', 'ed_org_id']
) %}
stu_long_subgroup as (
    {{ dbt_utils.unpivot(
       relation=ref('dim_student'),
       cast_to='string',
       exclude=[
          'k_student',
          'k_student_xyear',
          'tenant_code',
          'school_year',
          'student_unique_id',
          'first_name',
          'middle_name',
          'last_name',
          'display_name',
          'birth_date',
          'race_array',
          'safe_display_name'
       ],
       remove = stu_id_cols,
       field_name='subgroup_category',
       value_name='subgroup_value'
  ) }}
),

-- add a record for each student that marks them in the "all_students" group
-- this will make BI and analytics on "all students" easy
stu_all_group as (
  select
    dim_student.k_student,
    dim_student.k_student_xyear,
    dim_student.tenant_code,
    dim_student.school_year,
    'all_students' as subgroup_category,
    'all_students' as subgroup_value
  from dim_student
),

stu_long_subgroup_with_all as (
  select
    k_student,
    k_student_xyear,
    tenant_code,
    school_year,
    subgroup_category,
    subgroup_value
  from stu_long_subgroup
  union all
  select * from stu_all_group
),

keyed as (
  select
    stu_long_subgroup.k_student,
    stu_long_subgroup.k_student_xyear,
    dim_subgroup.k_subgroup,
    stu_long_subgroup.tenant_code,
    stu_long_subgroup.school_year

    -- custom data sources columns
    {{ add_cds_columns(cds_model_config=custom_data_sources_name) }}
  from stu_long_subgroup_with_all stu_long_subgroup
  -- todo: use dbt_utils.generate_surrogate_key() instead?
  -- lower() is because unpivot makes values capitalized
  join dim_subgroup
    on lower(stu_long_subgroup.subgroup_category) = lower(dim_subgroup.subgroup_category)
    and lower(stu_long_subgroup.subgroup_value) = lower(dim_subgroup.subgroup_value)
        
  -- custom data sources
  {{ add_cds_joins_v2(cds_model_config=custom_data_sources_name) }}
)

select * from keyed
order by tenant_code, school_year desc, k_student, k_subgroup