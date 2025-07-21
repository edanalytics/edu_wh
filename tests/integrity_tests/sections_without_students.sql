/*
## What is this test?
This test finds course section records with no students associated with them.

## When is this important to resolve?
TODO

## How to resolve?
TODO
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
)

select dim_course_section.*
from dim_course_section
left join fct_student_section_association 
    on fct_student_section_association.k_course_section = dim_course_section.k_course_section
where fct_student_section_association.k_course_section is null
