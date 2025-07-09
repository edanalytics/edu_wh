/*
## What is this test?
This test finds student enrollments without any overlapping course sections.

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
with dim_student as (
    select * from {{ ref("dim_student") }}
), fct_student_school_association as (
    select * from {{ ref("fct_student_school_association") }}
), fct_student_section_association as (
    select * from {{ ref("fct_student_section_association") }}
)


select fct_student_school_association.*
from dev_ac_wh.dim_student
inner join dev_ac_wh.fct_student_school_association
    on fct_student_school_association.k_student = dim_student.k_student
left join dev_ac_wh.fct_student_section_association
    on fct_student_section_association.k_student = dim_student.k_student
    and fct_student_section_association.k_school = fct_student_school_association.k_school
    -- add school year to join to be explicit?
    and (
        (
            fct_student_section_association.begin_date >= fct_student_school_association.entry_date
            and fct_student_section_association.begin_date <= coalesce(fct_student_school_association.exit_withdraw_date, current_date())
        )
        or (
            fct_student_section_association.end_date >= fct_student_school_association.entry_date
            and fct_student_section_association.end_date <= coalesce(fct_student_school_association.exit_withdraw_date, current_date())
        )
    )
where fct_student_section_association.k_student is null
