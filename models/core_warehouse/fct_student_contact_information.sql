with stu_phone_wide as (
    select * from {{ ref('bld_ef3__student_wide_phone_numbers') }}
),
stu_emails_wide as (
    select * from {{ ref('bld_ef3__student_wide_emails') }}
),
stu_address_wide as (
    select * from {{ ref('bld_ef3__student_wide_address') }}
),
dim_student as (
    select * from {{ ref('dim_student') }}
)
select 
    dim_student.k_student, 
    dim_student.k_student_xyear,
    dim_student.tenant_code, 
    dim_student.school_year,
    {{ accordion_columns(
            source_table='bld_ef3__student_wide_phone_numbers',
            exclude_columns=["k_student", "tenant_code"],
            source_alias='stu_phone_wide'
        ) }}
    {{ accordion_columns(
            source_table='bld_ef3__student_wide_emails',
            exclude_columns=["k_student", "tenant_code"],
            source_alias='stu_emails_wide'
    ) }} 
    {{ accordion_columns(
            source_table='bld_ef3__student_wide_address',
            exclude_columns=["k_student", "tenant_code"],
            source_alias='stu_address_wide'
    ) }} 
from dim_student 
left join stu_phone_wide on dim_student.k_student = stu_phone_wide.k_student 
left join stu_emails_wide on dim_student.k_student = stu_emails_wide.k_student 
left join stu_address_wide  on dim_student.k_student = stu_address_wide.k_student
