{{
  config(
    post_hook=[
        "alter table {{ this }} add primary key (k_student_academic_record)",
        "alter table {{ this }} add constraint fk_{{ this.name }}_student foreign key (k_student) references {{ ref('dim_student') }}",
    ]
  )
}}

with stg_academic_record as (
    select * from {{ ref('stg_ef3__student_academic_records') }}
),
dim_school as (
    select * from {{ ref('dim_school') }}
),
dim_student as (
    select * from {{ ref('dim_student') }}
),
most_recent_k_student as (
    select * from {{ ref('bld_ef3__most_recent_student_record') }}
),
formatted as (
    select 
        stg_academic_record.k_student_academic_record,
        stg_academic_record.k_student_xyear,
        most_recent_k_student.k_student,
        -- fill district if record is specified at school level
        coalesce(stg_academic_record.k_lea, dim_school.k_lea) as k_lea,
        stg_academic_record.k_school,
        stg_academic_record.tenant_code,
        stg_academic_record.school_year,
        stg_academic_record.academic_term,
        stg_academic_record.session_earned_credits,
        stg_academic_record.session_attempted_credits,
        stg_academic_record.cumulative_earned_credits,
        stg_academic_record.cumulative_attempted_credits,
        stg_academic_record.projected_graduation_date,
        stg_academic_record.class_rank,
        stg_academic_record.class_rank_total_students,
        stg_academic_record.class_percent_rank,
        stg_academic_record.class_rank_date,
        stg_academic_record.cumulative_earned_credit_type,
        stg_academic_record.cumulative_earned_credit_conversion,
        stg_academic_record.cumulative_attempted_credit_type,
        stg_academic_record.cumulative_attempted_credit_conversion,
        stg_academic_record.session_earned_credit_type,
        stg_academic_record.session_earned_credit_conversion,
        stg_academic_record.session_attempted_credit_type,
        stg_academic_record.session_attempted_credit_conversion
    from stg_academic_record
    left join dim_school
        on stg_academic_record.k_school = dim_school.k_school
    left join most_recent_k_student
        on stg_academic_record.k_student_xyear = most_recent_k_student.k_student_xyear
)
select * from formatted