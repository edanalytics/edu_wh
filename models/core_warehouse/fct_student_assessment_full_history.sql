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
{# find most recent k_student value for each stu, this is the k_stu we'll link
   historic assessments to #}
most_recent_k_student as (
    select
        k_student,
        k_student_xyear
    from dim_student
    qualify school_year = max(school_year) over (partition by k_student_xyear)
),
{# associate all historic assessment records with the most recent k_student for the given k_student_xyear.
   NOTE this includes both:
    a) stu-assess records that DO NOT have dim_student records for the year of the assessment
    b) stu-assess records that DO have dim_student records for the year of the assessment
#}
joined as (
    select
      bld_stu_assess.k_student_assessment,
      bld_stu_assess.k_assessment,
      most_recent_k_student.k_student,
      bld_stu_assess.school_year,
      {{ dbt_utils.star(from=ref('bld_ef3__student_assessment'),
                        except=['k_student_assessment','k_assessment','k_student', 'school_year'],
                        relation_alias='bld_stu_assess') }}
    from bld_stu_assess
    join most_recent_k_student 
      on bld_stu_assess.k_student_xyear = most_recent_k_student.k_student_xyear
)
select * from joined