{{
  config(
    post_hook=[
        "alter table {{ this }} alter column k_student_disability set not null",
        "alter table {{ this }} alter column k_student set not null",
        "alter table {{ this }} alter column disability_type set not null",
        "alter table {{ this }} add primary key (k_student_disability)",
        "alter table {{ this }} add constraint fk_{{ this.name }}_student foreign key (k_student) references {{ ref('dim_student') }}",
        "alter table {{ this }} add constraint fk_{{ this.name }}_program foreign key (k_program) references {{ ref('dim_program') }}",
        "alter table {{ this }} add constraint fk_{{ this.name }}_lea foreign key (k_lea) references {{ ref('dim_lea') }}",
        "alter table {{ this }} add constraint fk_{{ this.name }}_school foreign key (k_school) references {{ ref('dim_school') }}",
    ]
  )
}}

with student_disabilities as (
    select * from {{ ref('bld_ef3__student__disabilities') }}
),
dim_student as (
    select * from {{ ref('dim_student') }}
),
student_disability_designations as (
    select * from {{ ref('bld_ef3__student__wide_disability_designations') }}
),
formatted as (
    select
        {{ dbt_utils.generate_surrogate_key(
            ['student_disabilities.tenant_code',
            'student_disabilities.school_year',
            'student_disabilities.k_student',
            'student_disabilities.k_lea',
            'student_disabilities.k_school',
            'student_disabilities.k_program',
            'student_disabilities.program_enroll_begin_date',
            'student_disabilities.disability_type',]
        ) }} as k_student_disability,
        student_disabilities.k_student,
        dim_student.k_student_xyear,
        student_disabilities.k_lea,
        student_disabilities.k_school,
        student_disabilities.k_program,
        student_disabilities.k_student_program,
        student_disabilities.is_program,
        student_disabilities.program_enroll_begin_date,
        student_disabilities.program_enroll_end_date,
        student_disabilities.tenant_code,
        student_disabilities.api_year,
        student_disabilities.school_year,
        student_disabilities.disability_type,
        student_disabilities.disability_source_type,
        student_disabilities.disability_diagnosis,
        student_disabilities.order_of_disability,
        -- Pivot disability designation descriptors into boolean columns.
        {{ accordion_columns(
            source_table='bld_ef3__student__wide_disability_designations',
            exclude_columns=['tenant_code', 'api_year', 'school_year', 'k_student', 'k_lea', 'k_school', 'k_program', 'disability_type'],
            source_alias='student_disability_designations',
            add_trailing_comma=false
        ) }}
    from student_disabilities
    inner join dim_student
        on dim_student.k_student = student_disabilities.k_student
    left join student_disability_designations
        on student_disabilities.k_student = student_disability_designations.k_student
        and (student_disabilities.k_lea = student_disability_designations.k_lea or (student_disabilities.k_lea is null and student_disability_designations.k_lea is null))
        and (student_disabilities.k_school = student_disability_designations.k_school or (student_disabilities.k_school is null and student_disability_designations.k_school is null))
        and (student_disabilities.k_program = student_disability_designations.k_program or (student_disabilities.k_program is null and student_disability_designations.k_program is null))
        and student_disabilities.tenant_code = student_disability_designations.tenant_code
        and student_disabilities.school_year = student_disability_designations.school_year
        and student_disabilities.disability_type = student_disability_designations.disability_type
)
select * from formatted
