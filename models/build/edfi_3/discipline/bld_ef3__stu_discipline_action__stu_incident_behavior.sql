with stu_discipline_incident_behaviors_actions as (
    select * from {{ ref('stg_ef3__discipline_actions__student_discipline_incident_behaviors') }}
),
stu_discipline_incident_behavior as (
    select * from {{ ref('stg_ef3__student_discipline_incident_behavior_associations') }}
),
joined as (
    select
        stu_discipline_incident_behaviors_actions.tenant_code,
        stu_discipline_incident_behaviors_actions.k_student,
        stu_discipline_incident_behaviors_actions.k_student_xyear,
        stu_discipline_incident_behaviors_actions.discipline_action_id,
        stu_discipline_incident_behaviors_actions.discipline_date,
        array_agg(
            {{ dbt_utils.surrogate_key(
                ['stu_discipline_incident_behaviors_actions.k_student',
                'stu_discipline_incident_behavior.k_discipline_incident',
                'lower(stu_discipline_incident_behavior.behavior_type)']
            ) }}
        ) as k_student_discipline_incident_behavior_array
    from stu_discipline_incident_behaviors_actions
    join stu_discipline_incident_behavior
        on stu_discipline_incident_behaviors_actions.k_student = stu_discipline_incident_behavior.k_student
        and stu_discipline_incident_behaviors_actions.k_student_xyear = stu_discipline_incident_behavior.k_student_xyear
        and stu_discipline_incident_behaviors_actions.school_id = stu_discipline_incident_behavior.school_id
        and stu_discipline_incident_behaviors_actions.incident_id = stu_discipline_incident_behavior.incident_id
        -- due to the deprecated version where behavior type is not required,
        -- we need to be able to either merge by the behavior type or not
        and ifnull(stu_discipline_incident_behaviors_actions.behavior_type, '1') = iff(stu_discipline_incident_behaviors_actions.behavior_type is null, '1', stu_discipline_incident_behavior.behavior_type)
    {{ dbt_utils.group_by(n=5) }}
)
select * from joined