{{
  config(
    post_hook=[
        "alter table {{ this }} add primary key (k_student, k_student_xyear, k_cohort, cohort_begin_date)",
        "alter table {{ this }} add constraint fk_{{ this.name }}_student foreign key (k_student) references {{ ref('dim_student') }}",
        "alter table {{ this }} add constraint fk_{{ this.name }}_cohort foreign key (k_cohort) references {{ ref('dim_cohort') }}",
    ]
  )
}}

with stage as (
    select * from {{ ref('stg_ef3__student_cohort_associations') }}
),

dim_student as (
    select * from {{ ref('dim_student') }}
),

dim_cohort as (
    select * from {{ ref('dim_cohort') }}
),

formatted as (
    select
        dim_student.k_student,
        dim_student.k_student_xyear,
        dim_cohort.k_cohort,
        dim_cohort.k_lea,
        dim_cohort.k_school,

        stage.tenant_code,
        stage.school_year,
        stage.cohort_begin_date,
        stage.cohort_end_date

        {{ edu_edfi_source.extract_extension(model_name='stg_ef3__student_cohort_associations', flatten=False) }}

    from stage

        inner join dim_student
            on stage.k_student = dim_student.k_student

        inner join dim_cohort
            on stage.k_cohort = dim_cohort.k_cohort
)

select * from formatted