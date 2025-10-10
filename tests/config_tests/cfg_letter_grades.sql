{{
  config(
      store_failures = true,
      severity       = 'warn'
    )
}}
with stg_grades as (
    select * from {{ ref('stg_ef3__grades') }}
),
xwalk_letter_grades as (
    select * from {{ ref('xwalk_letter_grades') }}
),
joined as (
    select distinct
        stg_grades.tenant_code,
        stg_grades.api_year,
        stg_grades.letter_grade_earned
    from stg_grades 
    left join xwalk_letter_grades
        on lower(stg_grades.letter_grade_earned) = xwalk_letter_grades.letter_grade
    where xwalk_letter_grades.letter_grade is null
)
select count(*) as failed_row_count, tenant_code, api_year from joined
group by all
having count(*) > 1