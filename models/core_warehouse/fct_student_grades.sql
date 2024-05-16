{{
  config(
    post_hook=[
        "alter table {{ this }} add primary key (k_grading_period, k_student, k_school, k_course_section, grade_type)",
        "alter table {{ this }} add constraint fk_{{ this.name }}_grading_period foreign key (k_grading_period) references {{ ref('dim_grading_period') }}",
        "alter table {{ this }} add constraint fk_{{ this.name }}_student foreign key (k_student) references {{ ref('dim_student') }}",
        "alter table {{ this }} add constraint fk_{{ this.name }}_school foreign key (k_school) references {{ ref('dim_school') }}",
        "alter table {{ this }} add constraint fk_{{ this.name }}_course_section foreign key (k_course_section) references {{ ref('dim_course_section') }}",
    ]
  )
}}

with stg_grades as (
    select * from {{ ref('stg_ef3__grades') }}
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
letter_grade_xwalk as (
    select * from {{ ref('xwalk_letter_grades') }}
),
formatted as (
    select 
        dim_student.k_student,
        dim_course_section.k_course_section,
        dim_school.k_school,
        dim_grading_period.k_grading_period,
        stg_grades.grade_type,
        stg_grades.tenant_code,
        stg_grades.letter_grade_earned,
        stg_grades.numeric_grade_earned,
        stg_grades.diagnostic_statement,
        stg_grades.performance_base_conversion,
        letter_grade_xwalk.unweighted_gpa_points,
        letter_grade_xwalk.exclude_from_gpa,
        letter_grade_xwalk.is_dorf,
        letter_grade_xwalk.grade_sort_index
        {# add any extension columns configured from stg_ef3__grades #}
        {{ edu_edfi_source.extract_extension(model_name='stg_ef3__grades', flatten=False) }}
    from stg_grades
    join dim_student
        on stg_grades.k_student = dim_student.k_student
    join dim_school 
        on stg_grades.k_school = dim_school.k_school
    join dim_grading_period 
        on stg_grades.k_grading_period = dim_grading_period.k_grading_period
    join dim_course_section
        on stg_grades.k_course_section = dim_course_section.k_course_section
    left join letter_grade_xwalk
        on lower(stg_grades.letter_grade_earned) = letter_grade_xwalk.letter_grade
)
select * from formatted