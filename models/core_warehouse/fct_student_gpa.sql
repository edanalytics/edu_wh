{{
  config(
    post_hook=[
        "alter table {{ this }} add primary key (k_student_academic_record, gpa_type, is_cumulative)",
        "alter table {{ this }} add constraint fk_{{ this.name }}_academic_record foreign key (k_student_academic_record) references {{ ref('fct_student_academic_record') }}",
        "alter table {{ this }} add constraint fk_{{ this.name }}_student foreign key (k_student) references {{ ref('dim_student') }}",
    ]
  )
}}

with combined_gpas as (
    select * from {{ ref('bld_ef3__combine_gpas') }}
),
academic_record as (
    select * from {{ ref('fct_student_academic_record') }}
),
formatted as (
    select 
        academic_record.k_student_academic_record,
        academic_record.k_student,
        academic_record.k_student_xyear,
        academic_record.k_lea,
        academic_record.k_school,
        academic_record.tenant_code,
        academic_record.school_year,
        academic_record.academic_term,
        combined_gpas.gpa_type,
        combined_gpas.gpa_value,
        combined_gpas.is_cumulative,
        combined_gpas.max_gpa_value
    from combined_gpas 
    join academic_record
        on combined_gpas.k_student_academic_record = academic_record.k_student_academic_record
)
select * from formatted
order by tenant_code, k_student