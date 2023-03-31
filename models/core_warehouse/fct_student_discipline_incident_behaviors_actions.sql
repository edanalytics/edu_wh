-- depends_on: {{ ref('xwalk_discipline_behaviors') }}
-- depends_on: {{ ref('xwalk_discipline_actions') }}
{{
  config(
    post_hook=[
        "alter table {{ this }} add primary key (k_student, k_discipline_incident, k_discipline_actions_event)",
        "alter table {{ this }} add constraint fk_{{ this.name }}_student foreign key (k_student) references {{ ref('dim_student') }}",
        "alter table {{ this }} add constraint fk_{{ this.name }}_discipline_incidents foreign key (k_discipline_incident) references {{ ref('dim_discipline_incidents') }}"
    ]
  )
}}

with stu_discipline_incident_behaviors_actions as (
    select * from {{ ref('stg_ef3__discipline_actions__student_discipline_incident_behaviors') }}
),
fct_student_discipline_actions as (
    select * from {{ ref('fct_student_discipline_actions') }}
),
fct_student_discipline_incident_behaviors as (
    select * from {{ ref('fct_student_discipline_incident_behaviors') }}
),
formatted as (
    select
        stu_discipline_incident_behaviors_actions.k_student,
        fct_student_discipline_incident_behaviors.k_discipline_incident,
        fct_student_discipline_actions.k_discipline_actions_event,
        stu_discipline_incident_behaviors_actions.tenant_code,
        fct_student_discipline_incident_behaviors.behavior_type,
        fct_student_discipline_actions.discipline_action
    from stu_discipline_incident_behaviors_actions
    join fct_student_discipline_actions
        on stu_discipline_incident_behaviors_actions.k_student = fct_student_discipline_actions.k_student
        and stu_discipline_incident_behaviors_actions.discipline_action_id = fct_student_discipline_actions.discipline_action_id
        and stu_discipline_incident_behaviors_actions.discipline_date = fct_student_discipline_actions.discipline_date
    join fct_student_discipline_incident_behaviors
        on stu_discipline_incident_behaviors_actions.k_student = fct_student_discipline_incident_behaviors.k_student
        and stu_discipline_incident_behaviors_actions.school_id = fct_student_discipline_incident_behaviors.school_id
        and stu_discipline_incident_behaviors_actions.incident_id = fct_student_discipline_incident_behaviors.incident_id
        -- due to the deprecated version where behavior type is not required,
        -- we need to be able to either merge by the behavior type or not
        and ifnull(stu_discipline_incident_behaviors_actions.behavior_type, 1) = iff(stu_discipline_incident_behaviors_actions.behavior_type is null, 1, fct_student_discipline_incident_behaviors.behavior_type)
    -- We have a 'is_most_severe' flag in fct_student_discipline_action
    -- but multiple discipline events can be associated with a single incident
    -- so we are using similar logic but instead partitioning by the incident to grab the
    -- most severe discipline action for a single incident
    -- Note: we are ordering by both discipline and behavior severity order
    -- ^ this will keep a single row for a student, incident, and incident event with the most severe behaviors and actions
    {% if not dbt_utils.get_column_values(table=ref('xwalk_discipline_actions'), column='severity_order') and not dbt_utils.get_column_values(table=ref('xwalk_discipline_behaviors'), column='severity_order') %}
    -- We only want this table populated if both severity orders are also populated
    where 1 = 0
    {% else %}
    qualify 1 = row_number() over (partition by stu_discipline_incident_behaviors_actions.k_student, fct_student_discipline_incident_behaviors.k_discipline_incident order by fct_student_discipline_actions.severity_order desc, fct_student_discipline_incident_behaviors.severity_order desc)
    {% endif %}
)
select * from formatted