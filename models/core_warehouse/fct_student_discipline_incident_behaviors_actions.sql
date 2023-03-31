{{
  config(
    post_hook=[
        "alter table {{ this }} add primary key (k_student, k_discipline_incident, k_discipline_actions_event)",
        "alter table {{ this }} add constraint fk_{{ this.name }}_student foreign key (k_student) references {{ ref('dim_student') }}",
        "alter table {{ this }} add constraint fk_{{ this.name }}_discipline_incidents foreign key (k_discipline_incident) references {{ ref('dim_discipline_incidents') }}"
    ]
  )
}}


with fct_student_discipline_incident_behaviors as (
    select * from {{ ref('fct_student_discipline_incident_behaviors') }}
),
x as (
    select * from {{ ref('stg_ef3__discipline_actions__student_discipline_incident_behaviors') }}
),
y as (
    select * from {{ ref('fct_student_discipline_actions') }}
),
formatted as (
    select
        dim_student.k_student,
        fct_student_discipline_incident_behaviors.k_discipline_incident,
        y.k_discipline_actions_event,
        x.tenant_code,
        fct_student_discipline_incident_behaviors.behavior_type,
        y.discipline_action
    from x
    join y
        on x.k_student = y.k_student
        and x.discipline_action_id = y.discipline_action_id
        and x.discipline_date = y.discipline_date
    join fct_student_discipline_incident_behaviors
        on dim_student.k_student = fct_student_discipline_incident_behaviors.k_student
        and x.school_id = fct_student_discipline_incident_behaviors.school_id
        and x.incident_id = fct_student_discipline_incident_behaviors.incident_id
        -- due to the deprecated version where behavior type is not required,
        -- we need to be able to either merge by the behavior type or not
        and ifnull(x.behavior_type, 1) = iff(x.behavior_type is null, 1, fct_student_discipline_incident_behaviors.behavior_type)
    -- We have a 'is_most_severe' flag in fct_student_discipline_action
    -- but multiple discipline events can be associated with a single incident
    -- so we are using similar logic but instead partitioning by the incident to grab the
    -- most severe discipline action for a single incident
    -- Note: we are ordering by both discipline and behavior severity order
    -- ^ this will keep a single row for a student, incident, and incident event with the most severe behaviors and actions
    {% if dbt_utils.get_column_values(table=ref('xwalk_discipline_actions'), column='severity order')|length > 0 and dbt_utils.get_column_values(table=ref('xwalk_discipline_behaviors'), column='severity order')|length > 0 %}
    having 1 = row_number() over (partition by k_student, k_discipline_incident order by y.severity_order desc, fct_student_discipline_incident_behaviors.severity_order desc)
    {% else %}
    -- We only want this table populated if both severity orders are also populated
    where 1 = 0
    {% endif %}
)
select * from formatted