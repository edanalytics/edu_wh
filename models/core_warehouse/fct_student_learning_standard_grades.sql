{{
  config(
    post_hook=[
        "alter table {{ this }} alter column k_grading_period set not null",
        "alter table {{ this }} alter column k_student set not null",
        "alter table {{ this }} alter column k_school set not null",
        "alter table {{ this }} alter column k_course_section set not null",
        "alter table {{ this }} alter column grade_type set not null",
        "alter table {{ this }} alter column k_learning_standard set not null",
        "alter table {{ this }} add primary key (k_grading_period, k_student, k_school, k_course_section, grade_type, k_learning_standard)",
        "alter table {{ this }} add constraint fk_{{ this.name }}_grading_period foreign key (k_grading_period) references {{ ref('dim_grading_period') }}",
        "alter table {{ this }} add constraint fk_{{ this.name }}_student foreign key (k_student) references {{ ref('dim_student') }}",
        "alter table {{ this }} add constraint fk_{{ this.name }}_school foreign key (k_school) references {{ ref('dim_school') }}",
        "alter table {{ this }} add constraint fk_{{ this.name }}_course_section foreign key (k_course_section) references {{ ref('dim_course_section') }}",
        "alter table {{ this }} add constraint fk_{{ this.name }}_learning_standard foreign key (k_learning_standard) references {{ ref('dim_learning_standard') }}",
    ]
  )
}}

with stg_grades_learning_standards as (
    select * from {{ ref('stg_ef3__grades__learning_standards') }}
),
dim_learning_standard as (
    select * from {{ ref('dim_learning_standard') }}
),
dim_student as (
    select * from {{ ref('dim_student') }}
),
dim_school as (
    select * from {{ ref('dim_school') }}
),
dim_grading_period as (
    select * from {{ ref('dim_grading_period') }}
),
dim_course_section as (
    select * from {{ ref('dim_course_section') }}
),

formatted as (
    select 
        dim_student.k_student,
        dim_course_section.k_course_section,
        dim_school.k_school,
        dim_grading_period.k_grading_period,
        stg_grades_learning_standards.grade_type,
        dim_learning_standard.k_learning_standard,
        stg_grades_learning_standards.tenant_code,
        stg_grades_learning_standards.performance_base_conversion_descriptor,
        stg_grades_learning_standards.learning_standard_letter_grade_earned,
        stg_grades_learning_standards.learning_standard_numeric_grade_earned
        {# add any extension columns configured from stg_ef3__grades__learning_standards #}
        {{ edu_edfi_source.extract_extension(model_name='stg_ef3__grades__learning_standards', flatten=False) }}
    from stg_grades_learning_standards
    join dim_learning_standard
        on stg_grades_learning_standards.k_learning_standard = dim_learning_standard.k_learning_standard
    join dim_student
        on stg_grades_learning_standards.k_student = dim_student.k_student
    join dim_school 
        on stg_grades_learning_standards.k_school = dim_school.k_school
    join dim_grading_period 
        on stg_grades_learning_standards.k_grading_period = dim_grading_period.k_grading_period
    join dim_course_section
        on stg_grades_learning_standards.k_course_section = dim_course_section.k_course_section
)
select * from formatted