/*
## What is this test?
This test finds course section records with no staff associated with them.

## When is this important to resolve?
Missing staff-section associations may affect your ability to analyze 
the effects of different staff on course-level metrics.


## How to resolve?
Check if staff-section associations (StaffSectionAssociations) are being
populated properly.
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
fct_staff_section_association as (
    select * from {{ ref("fct_staff_section_association") }}
)

select dim_course_section.*
from dim_course_section
left join fct_staff_section_association 
    on fct_staff_section_association.k_course_section = dim_course_section.k_course_section
where fct_staff_section_association.k_course_section is null
