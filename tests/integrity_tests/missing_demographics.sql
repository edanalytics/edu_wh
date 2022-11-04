/*
Find students who have valid school enrollments, but are not in dim_student.
*/
{{
  config(
      store_failures = true,
      severity       = 'warn'
    )
}}
-- find students with school enrollments
with stu_with_enroll as (
  select distinct stu.tenant_code, stu.api_year, stu.k_student
  from {{ ref('stg_ef3__students') }} stu
  join {{ ref('stg_ef3__student_school_associations') }} ssa
      on stu.k_student = ssa.k_student
)
-- of these: which students are not in the dimension
select stu_with_enroll.*
from stu_with_enroll
left join {{ ref('dim_student') }}
    on stu_with_enroll.k_student = dim_student.k_student
where dim_student.k_student is null