with
    fct_student_school_association as (
        select * from {{ ref("fct_student_school_association") }}
    ),
    fct_student_section_association as (
        select * from {{ ref("fct_student_section_association") }}
    )

select
    fct_student_school_association.tenant_code,
    fct_student_school_association.k_school,
    fct_student_school_association.k_student,
    fct_student_school_association.school_year,
    fct_student_school_association.entry_date,
    count(fct_student_section_association.k_course_section) as n_sections
from fct_student_school_association
left join fct_student_section_association
    on fct_student_school_association.k_student = fct_student_section_association.k_student
    and fct_student_school_association.k_school = fct_student_section_association.k_school
    and (
        (
            fct_student_section_association.begin_date
            between fct_student_school_association.entry_date and coalesce(
                fct_student_school_association.exit_withdraw_date, current_date()
            )
        )
        or (
            fct_student_section_association.end_date
            between fct_student_school_association.entry_date and coalesce(
                fct_student_school_association.exit_withdraw_date, current_date()
            )
        )
    )
group by 1, 2, 3, 4
