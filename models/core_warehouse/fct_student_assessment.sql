{{
  config(
    post_hook=[
        "alter table {{ this }} add primary key (k_student_assessment)",
        "alter table {{ this }} add constraint fk_{{ this.name }}_assessment foreign key (k_assessment) references {{ ref('dim_assessment') }}",
        "alter table {{ this }} add constraint fk_{{ this.name }}_student foreign key (k_student) references {{ ref('dim_student') }}"
    ]
  )
}}

with bld_stu_assess as (
    select * from {{ ref('bld_ef3__student_assessment') }}
),
dim_student as (
    select * from {{ ref('dim_student') }}
),
{# inner join to dim student to drop assess records without stu rosters. those assessments
   will be represented in fct_student_historic_assessment #}
joined as (
    select
      bld_stu_assess.*
    from bld_stu_assess
    join dim_student 
      on bld_stu_assess.k_student = dim_student.k_student
)
select * from joined