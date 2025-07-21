/*
## What is this test?
This test finds schools with over 50% of student enrollments in any given
school year that don't have any overlapping course sections.

## When is this important to resolve?
This test flags that course enrollment data may be incomplete, which may be
important if you want to know which students were enrolled in which courses. 

## How to resolve?
TODO
*/

{{ 
  config(
    store_failures = true,
    severity = 'warn'
  )
}}

with sections_per_enrollment as (
    select *
    from {{ ref('sections_per_enrollment') }}
)

select k_school,
    school_year,
    count(1) as n_enrollments,
    sum(case when n_sections = 0 then 1 else 0 end) as n_enrollments_without_sections,
    n_enrollments_without_sections / n_enrollments as p_enrollments_without_sections
from sections_per_enrollment
group by 1, 2
having p_enrollments_without_sections > 0.5
