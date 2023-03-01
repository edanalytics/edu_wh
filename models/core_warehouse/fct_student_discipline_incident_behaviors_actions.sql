--todo: I hate this
--todo: I really hate this
with stu_discipline_incident_behaviors_actions as (
    select * from {{ ref('stg_ef3__discipline_actions__student_discipline_incident_behaviors') }}
),
fct_student_discipline_actions as (
    select * from {{ ref('fct_student_discipline_actions') }}
),
fct_student_discipline_incident_behaviors as (
    select * from {{ ref('fct_student_discipline_incident_behaviors') }}
),
dim_student as (
    select * from {{ ref('dim_student') }}
),
dim_school as (
    select * from {{ ref('dim_school') }}
),
formatted as (
    select
        dim_student_incident.k_student as k_student__incident,
        fct_student_discipline_incident_behaviors.k_discipline_incident,
        dim_student_action.k_student as k_student__action,
        stu_discipline_incident_behaviors_actions.k_discipline_action
        fct_student_discipline_incident_behaviors.behavior_type,
        fct_student_discipline_actions.discipline_action
    from stu_discipline_incident_behaviors_actions
    join fct_student_discipline_actions
        on stu_discipline_incident_behaviors_actions.k_student = fct_student_discipline_actions.k_student
        and stu_discipline_incident_behaviors_actions.discipline_action_id = fct_student_discipline_actions.discipline_action_id
        and stu_discipline_incident_behaviors_actions.discipline_date = fct_student_discipline_actions.discipline_date
    join dim_student as dim_student_incident
        on stu_discipline_incident_behaviors_actions.student_unique_id = dim_student_incident.student_unique_id
        and stu_discipline_incident_behaviors_actions.api_year = dim_student_incident.school_year
        and stu_discipline_incident_behaviors_actions.tenant_code = dim_student_incident.tenant_code
    join dim_student as dim_student_action
        on stu_discipline_incident_behaviors_actions.k_student = dim_student_action.k_student
    join fct_student_discipline_incident_behaviors
        -- note joining against dim student and not k student of stg table because that is k student action
        on dim_student_incident.k_student = fct_student_discipline_incident_behaviors.k_student
        and stu_discipline_incident_behaviors_actions.incident_id = fct_student_discipline_incident_behaviors.incident_id
        and ifnull(stu_discipline_incident_behaviors_actions.behavior_type, 1) = iff(stu_discipline_incident_behaviors_actions.behavior_type is null, 1, fct_student_discipline_incident_behaviors.behavior_type)
    join dim_school 
        on stu_discipline_incident_behaviors_actions.school_id = dim_school.school_id
        and stu_discipline_incident_behaviors_actions.tenant_code = dim_school.tenant_code
    -- todo: subset to non-offenders? (does this already happen with merge on discipline actions?)
    where fct_student_discipline_incident_behaviors.is_offender
)
select * from formatted