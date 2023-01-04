/*
Find students who have multiple primary contacts in the student parent associations.
*/
{{
  config(
      store_failures = true,
      severity       = 'warn'
    )
}}
-- find students with multiple primary contacts
with stu_with_multiple_primary_contacts as (
  select tenant_code, api_year, k_student, sum(is_primary_contact::int) as n_primary_contacts
  from {{ ref('fct_student_parent_association') }}
  group by tenant_code, api_year, k_student
  having n_primary_contacts > 1
)
select * from stu_with_multiple_primary_contacts