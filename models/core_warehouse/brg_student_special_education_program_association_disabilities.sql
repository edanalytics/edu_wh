{{
  config(
    post_hook=[
        "alter table {{ this }} alter column k_stu_sped_program_assoc set not null",
        "alter table {{ this }} alter column k_student set not null",
        "alter table {{ this }} alter column k_student_xyear set not null",
        "alter table {{ this }} alter column k_program set not null",
        "alter table {{ this }} alter column program_enroll_begin_date set not null",
        "alter table {{ this }} alter column disability_type set not null",
        "alter table {{ this }} add primary key (k_stu_sped_program_assoc, disability_type)",
        "alter table {{ this }} add constraint fk_{{ this.name }}_stu_sped_program_assoc foreign key (k_student, k_student_xyear, k_program, program_enroll_begin_date) references {{ ref('fct_student_special_education_program_association') }}",
        "alter table {{ this }} add constraint fk_{{ this.name }}_student foreign key (k_student) references {{ ref('dim_student') }}",
        "alter table {{ this }} add constraint fk_{{ this.name }}_program foreign key (k_program) references {{ ref('dim_program') }}",
    ]
  )
}}

with stage as (
    select * from {{ ref('stg_ef3__stu_spec_ed__disabilities') }}
),
disability_designations as (
    select * from {{ ref('bld_ef3__student_program__special_education__wide_disability_designations') }}
),
formatted as (
    select
        {{ dbt_utils.generate_surrogate_key(
            [
                'stage.tenant_code',
                'stage.school_year', 
                'stage.k_student',
                'stage.k_program', 
                'stage.k_lea',
                'stage.k_school',
                'stage.program_enroll_begin_date'
            ]
        )}} as k_stu_sped_program_assoc,
        stage.k_student,
        stage.k_student_xyear,
        stage.k_program,
        stage.k_lea,
        stage.k_school,
        stage.tenant_code,
        stage.school_year,
        stage.program_enroll_begin_date,
        stage.program_enroll_end_date,
        stage.disability_type,
        stage.disability_source_type,
        stage.disability_diagnosis,
        stage.order_of_disability,
        -- disability designations
        {{ accordion_columns(
            source_table='bld_ef3__student_program__special_education__wide_disability_designations',
            exclude_columns=['k_student', 'k_program', 'k_lea', 'k_school', 'tenant_code', 'school_year', 'program_enroll_begin_date', 'disability_type'],
            source_alias='disability_designations',
            add_trailing_comma=false
        ) }}
    from stage
    join disability_designations
        on stage.k_student = disability_designations.k_student
        and stage.k_program = disability_designations.k_program
        and coalesce(stage.k_lea, 'this_is_null') = coalesce(disability_designations.k_lea, 'this_is_null')
        and coalesce(stage.k_school, 'this_is_null') = coalesce(disability_designations.k_school, 'this_is_null')
        and stage.tenant_code = disability_designations.tenant_code
        and stage.school_year = disability_designations.school_year
        and stage.program_enroll_begin_date = disability_designations.program_enroll_begin_date
        and stage.disability_type = disability_designations.disability_type
)
select * from formatted