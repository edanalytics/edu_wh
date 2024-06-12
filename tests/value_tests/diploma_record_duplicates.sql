{{
  config(
      store_failures = true,
      severity       = 'warn'
    )
}}
with stg_diplomas as (
    select * from {{ ref('stg_ef3__student_academic_records__diplomas') }}
),
stu_academic_records as (
      select * from {{ ref('fct_student_academic_record') }}
), 
{% if var('edu:xwalk_academic_terms:enabled', False) %}
xwalk_academic_terms as (
    select * from {{ ref('xwalk_academic_terms') }}
),
{% endif %}
count_duplicates as (
    select
        stg_diplomas.tenant_code,  
        stu_academic_records.k_student,
        stu_academic_records.k_student_xyear,
        stu_academic_records.k_lea, 
        stu_academic_records.k_school, 
        stu_academic_records.school_year, 
        stu_academic_records.academic_term,
        diploma_type, 
        diploma_award_date, 
        count(*) over (partition by k_student, k_student_xyear, school_year, k_lea, k_school, diploma_type, diploma_award_date) as n_duplicates,
        row_number() over (partition by k_student, k_student_xyear, school_year, k_lea, k_school, diploma_type, diploma_award_date
            order by {% if var('edu:xwalk_academic_terms:enabled', False) %} coalesce(sort_index, 99) {% else %} academic_term {% endif %}) = 1 as is_kept_in_fct_student_diploma
    from stg_diplomas
    join stu_academic_records 
        on stg_diplomas.k_student_academic_record = stu_academic_records.k_student_academic_record
    {% if var('edu:xwalk_academic_terms:enabled', False) %}
    left join xwalk_academic_terms
        on stu_academic_records.academic_term = xwalk_academic_terms.academic_term
    {% endif %}
)
select * 
from count_duplicates
where n_duplicates > 1
order by tenant_code, k_student