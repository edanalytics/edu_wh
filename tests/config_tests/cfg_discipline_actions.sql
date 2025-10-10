{{
  config(
      store_failures = true,
      severity       = 'warn'
    )
}}
with fct_student_discipline_action as (
    select * from {{ ref('fct_student_discipline_actions') }}
),
xwalk_discipline_actions as (
    select * from {{ ref('xwalk_discipline_actions') }}
),
joined as (
    select distinct
        fct_student_discipline_action.tenant_code,
        fct_student_discipline_action.school_year,
        fct_student_discipline_action.discipline_action
    from fct_student_discipline_action
    left join xwalk_discipline_actions
        on fct_student_discipline_action.discipline_action = xwalk_discipline_actions.discipline_action
    where xwalk_discipline_actions.severity_order is null
)
select count(*) as failed_row_count, tenant_code, school_year from joined
group by all
having count(*) > 1