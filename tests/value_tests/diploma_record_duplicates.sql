/*
**What is this test?**
This test finds records where the same diploma type is repeated across every term. 
This test shows how the deduplication is being handled in fct_student_diploma, but the duplicates
may point to data quality issues that could be addressed in the source system or ODS.

**When is this important to resolve?**
This data came from student academic records, which are linked to academic terms. This means 
that the same diploma data is duplicated on every academic term of the school year when it 
was awarded. If this test determines that a diploma has been duplicated many times over, it 
is important to resolve this data issue so to avoid a warehouse model from blowing up. 

**How to resolve?**
Determine whether this test has uncovered the behavior of these duplicates. 
Depending on the diagnosis from the underlying data, you may raise issue in the source system.  

*/

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
            order by {% if var('edu:xwalk_academic_terms:enabled', False) %} sort_index nulls last {% else %} academic_term {% endif %}) = 1 as is_kept_in_fct_student_diploma
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