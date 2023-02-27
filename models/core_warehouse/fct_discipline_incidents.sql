{{
  config(
    post_hook=[
        "alter table {{ this }} add primary key (k_school, incident_id)",
        "alter table {{ this }} add constraint fk_{{ this.name }}_school foreign key (k_school) references {{ ref('dim_school') }}",
    ]
  )
}}

with stg_discipline_incidents as (
    select * from {{ ref('stg_ef3__discipline_incidents') }}
),
dim_school as (
    select * from {{ ref('dim_school') }}
),
behaviors as (
    select
        k_discipline_incident,
        array_agg(object_construct('behavior_type', behavior_type,
                                   'behavior_detailed_description', behavior_detailed_description)) as behavior_array
    from {{ ref('stg_ef3__discipline_incidents__behaviors') }}
    group by k_discipline_incident
),
formatted as (
    select
        stg_discipline_incidents.k_discipline_incident,
        dim_school.k_school,
        stg_discipline_incidents.tenant_code,
        stg_discipline_incidents.incident_id,
        stg_discipline_incidents.incident_date,
        -- adding an indicator for multiple behaviors for an incident
        case
            when array_size(stg_discipline_incidents.v_behaviors) > 1
                then true
            else false 
        end as has_multiple_behaviors,
        stg_discipline_incidents.case_number,
        stg_discipline_incidents.incident_cost,
        stg_discipline_incidents.incident_description,
        stg_discipline_incidents.was_reported_to_law_enforcement,
        stg_discipline_incidents.reporter_name,
        stg_discipline_incidents.reporter_description,
        stg_discipline_incidents.incident_location,
        behaviors.behavior_array
        -- todo: weapons? external participants?
    from stg_discipline_incidents
    -- behaviors are not required
    left join behaviors
        on stg_discipline_incidents.k_discipline_incident = behaviors.k_discipline_incident
    join dim_school
        on stg_discipline_incidents.k_school = dim_school.k_school
)
select * from formatted