{{
  config(
    post_hook=[
        "alter table {{ this }} add primary key (k_student, k_student_xyear, k_discipline_actions_event)",
        "alter table {{ this }} add constraint fk_{{ this.name }}_student foreign key (k_student) references {{ ref('dim_student') }}"
    ]
  )
}}

with stu_discipline_incident_behaviors_actions as (
    select * from {{ ref('stg_ef3__discipline_actions__student_discipline_incident_behaviors') }}
),
stu_discipline_actions_disciplines as (
    select * from {{ ref('stg_ef3__discipline_actions__disciplines') }}
),
fct_student_discipline_incident_behaviors as (
    select * from {{ ref('fct_student_discipline_incident_behaviors') }}
),
fct_student_discipline_actions as (
    select * from {{ ref('fct_student_discipline_actions') }}
),
behaviors_array as (
    -- goal here is to find all behaviors for an action ID, regardless of whether they are linked to
    -- different incident IDs
    select
        stu_discipline_incident_behaviors_actions.k_student,
        stu_discipline_incident_behaviors_actions.k_student_xyear,
        stu_discipline_incident_behaviors_actions.discipline_action_id,
        stu_discipline_incident_behaviors_actions.discipline_date,
        array_agg(fct_student_discipline_incident_behaviors.behavior_type) as behavior_types_array
    from stu_discipline_incident_behaviors_actions
    join fct_student_discipline_incident_behaviors
        on stu_discipline_incident_behaviors_actions.k_student = fct_student_discipline_incident_behaviors.k_student
        and stu_discipline_incident_behaviors_actions.k_student_xyear = fct_student_discipline_incident_behaviors.k_student_xyear
        and stu_discipline_incident_behaviors_actions.school_id = fct_student_discipline_incident_behaviors.school_id
        and stu_discipline_incident_behaviors_actions.incident_id = fct_student_discipline_incident_behaviors.incident_id
        -- due to the deprecated version where behavior type is not required,
        -- we need to be able to either merge by the behavior type or not
        and ifnull(stu_discipline_incident_behaviors_actions.behavior_type, '1') = iff(stu_discipline_incident_behaviors_actions.behavior_type is null, '1', fct_student_discipline_incident_behaviors.behavior_type)
    group by 1,2,3,4
),
actions_array as (
    select
        k_student,
        k_student_xyear,
        k_discipline_actions_event,
        array_agg(discipline_action) as discipline_actions_array
    from fct_student_discipline_actions
    group by 1,2,3
),
formatted as (
    select
        stu_discipline_incident_behaviors_actions.k_student,
        stu_discipline_incident_behaviors_actions.k_student_xyear,
        fct_student_discipline_actions.k_discipline_actions_event,
        stu_discipline_incident_behaviors_actions.tenant_code,
        behaviors_array.behavior_types_array,
        actions_array.discipline_actions_array,
        -- we want to include the most severe behavior type and discipline action
        -- only if both severity orders are defined
        iff(fct_student_discipline_incident_behaviors.severity_order is not null and 
            fct_student_discipline_actions.severity_order is not null, 
            fct_student_discipline_incident_behaviors.behavior_type, null) as most_severe_behavior_type,
        iff(fct_student_discipline_incident_behaviors.severity_order is not null and 
            fct_student_discipline_actions.severity_order is not null, 
            fct_student_discipline_actions.discipline_action, null) as most_severe_discipline_action
    from stu_discipline_incident_behaviors_actions
    join fct_student_discipline_actions
        on stu_discipline_incident_behaviors_actions.k_student = fct_student_discipline_actions.k_student
        and stu_discipline_incident_behaviors_actions.k_student_xyear = fct_student_discipline_actions.k_student_xyear
        and stu_discipline_incident_behaviors_actions.discipline_action_id = fct_student_discipline_actions.discipline_action_id
        and stu_discipline_incident_behaviors_actions.discipline_date = fct_student_discipline_actions.discipline_date
    join fct_student_discipline_incident_behaviors
        on stu_discipline_incident_behaviors_actions.k_student = fct_student_discipline_incident_behaviors.k_student
        and stu_discipline_incident_behaviors_actions.k_student_xyear = fct_student_discipline_incident_behaviors.k_student_xyear
        and stu_discipline_incident_behaviors_actions.school_id = fct_student_discipline_incident_behaviors.school_id
        and stu_discipline_incident_behaviors_actions.incident_id = fct_student_discipline_incident_behaviors.incident_id
        -- due to the deprecated version where behavior type is not required,
        -- we need to be able to either merge by the behavior type or not
        and ifnull(stu_discipline_incident_behaviors_actions.behavior_type, '1') = iff(stu_discipline_incident_behaviors_actions.behavior_type is null, '1', fct_student_discipline_incident_behaviors.behavior_type)
    join behaviors_array 
        on fct_student_discipline_actions.k_student = behaviors_array.k_student
        and fct_student_discipline_actions.k_student_xyear = behaviors_array.k_student_xyear
        and fct_student_discipline_actions.discipline_action_id = behaviors_array.discipline_action_id
        and fct_student_discipline_actions.discipline_date = behaviors_array.discipline_date
    join actions_array
        on fct_student_discipline_actions.k_student = actions_array.k_student
        and fct_student_discipline_actions.k_student_xyear = actions_array.k_student_xyear
        and fct_student_discipline_actions.k_discipline_actions_event = actions_array.k_discipline_actions_event
    -- in order to keep the grain of k_student and k_discipline_actions_event, we want to keep this subset 
    -- even if we do not have the severity orders defined
    qualify 1 = row_number() over (partition by stu_discipline_incident_behaviors_actions.k_student, fct_student_discipline_actions.k_discipline_actions_event order by fct_student_discipline_actions.severity_order desc nulls last, fct_student_discipline_incident_behaviors.severity_order desc nulls last)
)
select * from formatted