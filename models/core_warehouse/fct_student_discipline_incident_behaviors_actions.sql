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
        dim_student.k_student as k_student,
        fct_student_discipline_incident_behaviors.k_discipline_incident,
        fct_student_discipline_actions.k_discipline_event,
        fct_student_discipline_incident_behaviors.behavior_type,
        fct_student_discipline_actions.discipline_action
    from stu_discipline_incident_behaviors_actions
    join fct_student_discipline_actions
        on stu_discipline_incident_behaviors_actions.k_student = fct_student_discipline_actions.k_student
        and stu_discipline_incident_behaviors_actions.discipline_action_id = fct_student_discipline_actions.discipline_action_id
        and stu_discipline_incident_behaviors_actions.discipline_date = fct_student_discipline_actions.discipline_date
    join dim_student
        on stu_discipline_incident_behaviors_actions.k_student = dim_student.k_student
    join fct_student_discipline_incident_behaviors
        on dim_student.k_student = fct_student_discipline_incident_behaviors.k_student
        and stu_discipline_incident_behaviors_actions.incident_id = fct_student_discipline_incident_behaviors.incident_id
        and ifnull(stu_discipline_incident_behaviors_actions.behavior_type, 1) = iff(stu_discipline_incident_behaviors_actions.behavior_type is null, 1, fct_student_discipline_incident_behaviors.behavior_type)
    join dim_school 
        on stu_discipline_incident_behaviors_actions.school_id = dim_school.school_id
        and stu_discipline_incident_behaviors_actions.tenant_code = dim_school.tenant_code
    -- We have a 'is_most_severe' flag in fct_student_discipline_action
    -- but multiple discipline events can be associated with a single incident
    -- so we are using similar logic but instead partitioning by the incident to grab the
    -- most severe discipline action for a single incident
    -- TODO: THIS WILL ALSO SUBSET TO A SINGLE BEHAVIOR I THINK WHICH WE DO NOT WANT
    -- ^ DO I INCLUDE BEHAVIOR TYPE IN THE PARTITION BY?
    -- or should we also have a severity order for behaviors?
    -- todo: what if severity order is not added?
    having 1 = row_number() over (partition by k_student, k_discipline_incident order by fct_student_discipline_actions.severity_order desc, fct_student_discipline_incident_behaviors.severity_order desc)

)
select * from formatted