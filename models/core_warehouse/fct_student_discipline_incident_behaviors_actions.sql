--todo: I hate this
--todo: I really hate this
with stu_discipline_incident_behaviors_actions as (
    select * from {{ ref('stg_ef3__discipline_actions__student_discipline_incident_behaviors') }}
),
student_discipline_incident_behaviors as (
    select * from {{ ref('stg_ef3__student_discipline_incident_behaviors') }}
),
dim_student as (
    select * from {{ ref('dim_student') }}
),
dim_school as (
    select * from {{ ref('dim_school') }}
),
--todo: still hate this
-- this doesn't even match the grain of stu discipline actions because it doesn't include the actual action
formatted as (
    select
        dim_student_incident.k_student as k_student__incident,
        dim_school.k_school as k_school_incident,
        dim_student_action.k_student as k_student__action,
        stu_discipline_incident_behaviors_actions.incident_id,
        stu_discipline_incident_behaviors_actions.behavior_type,
        stu_discipline_incident_behaviors_actions.discipline_action_id,
        stu_discipline_incident_behaviors_actions.discipline_date
    from stu_discipline_incident_behaviors_actions
    join dim_student as dim_student_incident
        on stu_discipline_incident_behaviors_actions.student_unique_id = dim_student_incident.student_unique_id
        and stu_discipline_incident_behaviors_actions.tenant_code = dim_student_incident.tenant_code
    join dim_student as dim_student_action
        on stu_discipline_incident_behaviors_actions.k_student = dim_student_action.k_student
    join dim_school 
        on stu_discipline_incident_behaviors_actions.school_id = dim_school.school_id
        and stu_discipline_incident_behaviors_actions.tenant_code = dim_school.tenant_code
)
select * from formatted