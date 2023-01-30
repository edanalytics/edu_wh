/*
Find students parent associations with multiple school years.
*/
{{
  config(
      store_failures = true,
      severity       = 'error'
    )
}}
-- Find students parent associations with multiple school years.
with stu_parent_associations_with_multiple_school_years as (
  select tenant_code, k_student_xyear, k_parent, count(distinct school_year) as n_school_years
  from {{ ref('fct_student_parent_association') }}
  group by tenant_code, k_student_xyear, k_parent
  having n_school_years > 1
)
select * from stu_parent_associations_with_multiple_school_years