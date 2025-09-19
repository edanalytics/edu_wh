{{
  config(
    post_hook=[
        "alter table {{ this }} alter column k_discipline_incident set not null",
        "alter table {{ this }} add primary key (k_discipline_incident)",
        "alter table {{ this }} add constraint fk_{{ this.name }}_school foreign key (k_school) references {{ ref('dim_school') }}",
    ]
  )
}}

{{ cds_depends_on('edu:discipline_incident:custom_data_sources') }}
{% set custom_data_sources = var('edu:discipline_incident:custom_data_sources', []) %}

with stg_discipline_incidents as (
    select * from {{ ref('stg_ef3__discipline_incidents') }}
),
dim_school as (
    select * from {{ ref('dim_school') }}
),
behaviors as (
    select
        k_discipline_incident,
        parse_json(
            to_json(
                array_agg(
                    named_struct('behavior_type', behavior_type, 'behavior_detailed_description', behavior_detailed_description)
                )
            )
        )  as behavior_array
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
        stg_discipline_incidents.incident_time,
        -- adding an indicator for multiple behaviors for an incident
        case
            when size(try_cast(stg_discipline_incidents.v_behaviors as array<string>)) > 1
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
        
        -- custom data sources columns
        {{ add_cds_columns(custom_data_sources=custom_data_sources) }}
    from stg_discipline_incidents
    -- behaviors are not required
    left join behaviors
        on stg_discipline_incidents.k_discipline_incident = behaviors.k_discipline_incident
    join dim_school
        on stg_discipline_incidents.k_school = dim_school.k_school

    -- custom data sources
    {{ add_cds_joins_v1(custom_data_sources=custom_data_sources, driving_alias='stg_discipline_incidents', join_cols=['k_discipline_incident']) }}
    {{ add_cds_joins_v2(custom_data_sources=custom_data_sources) }}
)
select * from formatted