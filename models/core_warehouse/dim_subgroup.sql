{{
  config(
    tags=['bypass_rls']
    )
}}
with dim_student as (
    select * from {{ ref('dim_student') }}
),
xwalk_subgroup_display_names as (
    select * from {{ ref('xwalk_subgroup_value_display_names') }}
),
xwalk_subgroup_category_display_names as (
    select * from {{ ref('xwalk_subgroup_category_display_names') }}
),

{% set stu_id_cols = dbt_utils.get_filtered_columns_in_relation(
        ref('bld_ef3__wide_ids_student'),
        except=['tenant_code', 'api_year', 'k_student', 'k_student_xyear', 'ed_org_id']
) %}
stu_long_subgroup as (
    {{ dbt_utils.unpivot(
       relation=ref('dim_student'),
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
          'safe_display_name',
          'is_latest_record'
       ],
       remove = stu_id_cols,
       field_name='subgroup_category',
       value_name='subgroup_value'
  ) }}
),

distinct_subgroups as (
  select distinct
    lower(subgroup_category) as subgroup_category,
    subgroup_value
  from stu_long_subgroup
),

-- add a subgroup for "all students" (every student will receive this in fct_student_subgroup)
-- this will make BI and analytics on "all students" easy
all_students_group as (
  select
    'all_students' as subgroup_category,
    'all_students' as subgroup_value
),

distinct_subgroups_with_all as (
  select * from distinct_subgroups
  union all
  select * from all_students_group
),

joined_with_display_names as (
  select
    distinct_subgroups.subgroup_category,
    coalesce(xwalk_subgroup_category_display_names.subgroup_category_display_name, distinct_subgroups.subgroup_category) as subgroup_category_display_name,
    distinct_subgroups.subgroup_value,
    coalesce(xwalk_subgroup_display_names.subgroup_value_display_name,  distinct_subgroups.subgroup_value) as subgroup_value_display_name
  from distinct_subgroups_with_all distinct_subgroups
  left join xwalk_subgroup_display_names
    on distinct_subgroups.subgroup_category = xwalk_subgroup_display_names.subgroup_category
    and distinct_subgroups.subgroup_value = xwalk_subgroup_display_names.subgroup_value
  left join xwalk_subgroup_category_display_names
    on distinct_subgroups.subgroup_category = xwalk_subgroup_category_display_names.subgroup_category

),

keyed as (
  select
    {{ dbt_utils.surrogate_key(['subgroup_category', 'subgroup_value'])}} as k_subgroup,
    joined_with_display_names.subgroup_category,
    joined_with_display_names.subgroup_category_display_name,
    joined_with_display_names.subgroup_value,
    joined_with_display_names.subgroup_value_display_name
  from joined_with_display_names
)

select * from keyed
order by subgroup_category, subgroup_value
