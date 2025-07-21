/*
## What is this test?
This test finds student enrollments without any overlapping course sections.

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
    k_student,
    school_year,
    entry_date,
    n_sections
from sections_per_enrollment
where n_sections = 0
