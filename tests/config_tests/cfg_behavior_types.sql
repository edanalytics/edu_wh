{{
  config(
      store_failures = true,
      severity       = 'warn'
    )
}}
with fct_student_discipline_incident_behaviors as (
    select * from {{ ref('fct_student_discipline_incident_behaviors') }}
),
dim_student as (
    select * from {{ ref('dim_student')}}
),
xwalk_discipline_behaviors as (
    select * from {{ ref('xwalk_discipline_behaviors') }}
),
joined as (
    select distinct
        fct_student_discipline_incident_behaviors.tenant_code,
        dim_student.school_year,
        fct_student_discipline_incident_behaviors.behavior_type
    from fct_student_discipline_incident_behaviors
    left join xwalk_discipline_behaviors
        on fct_student_discipline_incident_behaviors.behavior_type = xwalk_discipline_behaviors.behavior_type
    join dim_student
        on fct_student_discipline_incident_behaviors.k_student = dim_student.k_student
    where xwalk_discipline_behaviors.severity_order is null

)
select count(*) as failed_row_count, tenant_code, school_year from joined
group by all
having count(*) > 1