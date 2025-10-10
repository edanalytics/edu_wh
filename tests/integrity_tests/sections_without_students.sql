/*
## What is this test?
This test finds course section records with no students associated with them.

## When is this important to resolve?
Rostering data may be important for analyzing section-level metrics.

## How to resolve?
Check if section enrollments (StudentSectionAssociations) are being populated
properly.
*/
{{ 
  config(
    store_failures = true,
    severity = 'warn'
  )
}}
with dim_course_section as (
    select * from {{ ref("dim_course_section") }}
),
fct_student_section_association as (
    select * from {{ ref("fct_student_section_association") }}
), 
joined as (
  select dim_course_section.*
  from dim_course_section
  left join fct_student_section_association 
      on fct_student_section_association.k_course_section = dim_course_section.k_course_section
  where fct_student_section_association.k_course_section is null
)
select count(*) as failed_row_count, tenant_code, school_year from joined
group by all
having count(*) > 1