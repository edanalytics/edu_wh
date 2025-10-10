/*
## What is this test?
This test finds student enrollments without any overlapping course sections.

## When is this important to resolve?
This test flags that course enrollment data may be incomplete, which may be
important if you want to know which students were enrolled in which courses. 

## How to resolve?
Check if student section enrollments (StudentSectionAssociations) are being
populated properly.
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
select count(*) as failed_row_count, tenant_code, school_year from sections_per_enrollment
where n_sections = 0
group by all
having count(*) > 1