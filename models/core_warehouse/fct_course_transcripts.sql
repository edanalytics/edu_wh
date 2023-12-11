{{
  config(
    post_hook=[
        "alter table {{ this }} add primary key (k_course, k_student_academic_record, course_attempt_result)",
        "alter table {{ this }} add constraint fk_{{ this.name }}_course foreign key (k_course) references {{ ref('dim_course') }}",
        "alter table {{ this }} add constraint fk_{{ this.name }}_academic_record foreign key (k_student_academic_record) references {{ ref('fct_student_academic_record') }}",
        "alter table {{ this }} add constraint fk_{{ this.name }}_student foreign key (k_student) references {{ ref('dim_student') }}",
        "alter table {{ this }} add constraint fk_{{ this.name }}_school foreign key (k_school) references {{ ref('dim_school') }}",
    ]
  )
}}

with course_transcripts as (
    select * from {{ ref('stg_ef3__course_transcripts') }}
),
dim_course as (
    select * from {{ ref('dim_course') }}
),
fct_student_academic_record as (
    select * from {{ ref('fct_student_academic_record') }}
),
dim_student as (
    select * from {{ ref('dim_student') }}
),
most_recent_k_student as (
    select 
        k_student_xyear,
        k_student
    from dim_student
    qualify school_year = max(school_year) over (partition by k_student_xyear) 
),
formatted as (
    select 
        course_transcripts.k_course,
        fct_student_academic_record.k_student_academic_record,
        fct_student_academic_record.k_lea,
        fct_student_academic_record.k_school,
        fct_student_academic_record.k_student_xyear,
        most_recent_k_student.k_student,
        course_transcripts.tenant_code,
        fct_student_academic_record.school_year,
        fct_student_academic_record.academic_term,
        course_transcripts.course_attempt_result,
        course_transcripts.course_title,
        course_transcripts.alternative_course_code,
        course_transcripts.alternative_course_title,
        course_transcripts.when_taken_grade_level,
        course_transcripts.final_letter_grade_earned,
        course_transcripts.final_numeric_grade_earned,
        course_transcripts.earned_credits,
        course_transcripts.attempted_credits,
        course_transcripts.course_repeat_code,
        course_transcripts.method_credit_earned,
        course_transcripts.earned_credit_type,
        course_transcripts.earned_credit_conversion,
        course_transcripts.attempted_credit_type,
        course_transcripts.attempted_credit_conversion,
        course_transcripts.assigning_organization_identification_code,
        course_transcripts.course_catalog_url
    from course_transcripts
    join fct_student_academic_record
        on course_transcripts.k_student_academic_record = fct_student_academic_record.k_student_academic_record
    join most_recent_k_student
        on stg_academic_record.k_student_xyear = most_recent_k_student.k_student_xyear
)
select * from formatted