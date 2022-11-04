{{
  config(
      store_failures = true,
      severity       = 'warn'
    )
}}
with dim_calendar_date as (
    select * from {{ ref('dim_calendar_date') }}
),
fct_student_school_assoc as (
    select * from {{ ref('fct_student_school_association') }}
),
counted as (
    select 
        fct_student_school_assoc.k_school, 
        fct_student_school_assoc.k_student,
        dim_calendar_date.calendar_date,
        count(*) as duplicate_days
    from dim_calendar_date
    join fct_student_school_assoc
        on dim_calendar_date.k_school_calendar = fct_student_school_assoc.k_school_calendar
        and dim_calendar_date.calendar_date between 
            fct_student_school_assoc.entry_date and
            coalesce(fct_student_school_assoc.exit_withdraw_date, current_date())
    group by 1,2,3
    having duplicate_days > 1
),
summarized as (
    select 
        k_school,
        k_student,
        count(distinct calendar_date)
    from counted
    group by 1,2
)
select * from summarized