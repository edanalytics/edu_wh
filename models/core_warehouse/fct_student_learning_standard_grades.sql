{{
  config(
    post_hook=[
        "alter table {{ this }} add primary key (k_grading_period, k_student, k_school, k_course_section, grade_type, k_learning_standard)",
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
stg_learning_standards as (
    select * from {{ ref('stg_ef3__learning_standards') }}
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

--this has to be outside the staging model because it changes the grain of the table
--from k_student, k_school, k_grading_period, k_course_section, grade_type
--to k_student, k_school, k_grading_period, k_course_section, grade_type, learning_standard_id
flattened as (
    select
        
        stg_grades.* exclude(letter_grade_earned, numeric_grade_earned),
        v_lsg.value:learningStandardReference:learningStandardId::string as learning_standard_id,
        v_lsg.value:letterGradeEarned::string as letter_grade_earned,
        v_lsg.value:numericGradeEarned::string as numeric_grade_earned,
        {{ edu_edfi_source.extract_descriptor('v_lsg.value:performanceBaseConversionDescriptor::string') }} as performance_base_conversion_descriptor
    from stg_grades,
        lateral flatten(input=>v_learning_standard_grades, outer=>true) as v_lsg
),

formatted as (
    select 
        dim_student.k_student,
        dim_course_section.k_course_section,
        dim_school.k_school,
        dim_grading_period.k_grading_period,
        stg_learning_standards.k_learning_standard,
        stg_learning_standards.learning_standard_id,
        stg_learning_standards.academic_subject_descriptor,
        flattened.grading_period_descriptor,
        flattened.grade_type,
        flattened.tenant_code,
        flattened.performance_base_conversion_descriptor as performance_base_conversion,
        flattened.letter_grade_earned,
        flattened.numeric_grade_earned
    from flattened
    join stg_learning_standards
        on flattened.learning_standard_id = stg_learning_standards.learning_standard_id
        and flattened.api_year = stg_learning_standards.api_year
        and flattened.tenant_code = stg_learning_standards.tenant_code
    join dim_student
        on flattened.k_student = dim_student.k_student
    join dim_school 
        on flattened.k_school = dim_school.k_school
    join dim_grading_period 
        on flattened.k_grading_period = dim_grading_period.k_grading_period
    join dim_course_section
        on flattened.k_course_section = dim_course_section.k_course_section
)
select * from formatted