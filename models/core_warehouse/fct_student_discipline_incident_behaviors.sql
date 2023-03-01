with stg_stu_discipline_incident_behaviors as (
    select * from {{ ref('stg_ef3__student_discipline_incident_behavior_associations') }}
),
stg_stu_discipline_incident_non_offenders as (
    select * from {{ ref('stg_ef3__student_discipline_incident_non_offender_associations') }}
),
dim_student as (
    select * from {{ ref('dim_student') }}
),
dim_school as (
    select * from {{ ref('dim_school') }}
),
dim_discipline_incidents as (
    select * from {{ ref('dim_discipline_incidents') }}
),
stack_discipline_incidents as (
    select
        stg_stu_discipline_incident_behaviors.k_student,
        stg_stu_discipline_incident_behaviors.k_school,
        stg_stu_discipline_incident_behaviors.k_discipline_incident,
        stg_stu_discipline_incident_behaviors.incident_id,
        stg_stu_discipline_incident_behaviors.behavior_type,
        stg_stu_discipline_incident_behaviors.behavior_detailed_description,
        -- todo: how do I deal with the list of participation codes
        -- don't want to change the grain
        -- do I pull one out somehow? by severity?
        true as is_offender
    from stg_stu_discipline_incident_behaviors

    union all 

    select
        stg_stu_discipline_incident_non_offenders.k_student,
        stg_stu_discipline_incident_non_offenders.k_school,
        stg_stu_discipline_incident_non_offenders.k_discipline_incident,
        stg_stu_discipline_incident_non_offenders.incident_id,
        null as behavior_type,
        null as behavior_detailed_description,
        -- todo: how do I deal with the list of participation codes
        -- don't want to change the grain
        -- do I pull one out somehow? by severity?
        false as is_offender
    from stg_stu_discipline_incident_non_offenders
),
formatted as (
    select 
        dim_student.k_student,
        dim_school.k_school,
        dim_discipline_incidents.k_discipline_incident,
        stack_discipline_incidents.incident_id,
        stack_discipline_incidents.behavior_type,
        stack_discipline_incidents.behavior_detailed_description,
        stack_discipline_incidents.is_offender
    from stack_discipline_incidents
    join dim_student on stack_discipline_incidents.k_student = dim_student.k_student
    join dim_school on stack_discipline_incidents.k_school = dim_school.k_school
    join dim_discipline_incidents on stack_discipline_incidents.k_discipline_incident = dim_discipline_incidents.k_discipline_incident
)
select *
from formatted