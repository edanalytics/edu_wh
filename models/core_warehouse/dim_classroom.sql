{{
  config(
    post_hook=[
        "alter table {{ this }} alter column k_classroom set not null",
        "alter table {{ this }} add primary key (k_classroom)",
        "alter table {{ this }} add constraint fk_{{ this.name }}_school foreign key (k_school) references {{ ref('dim_school') }}",
    ]
  )
}}

{% set custom_data_sources_name = "edu:classroom:custom_data_sources" %}

with locations as (
    select * from {{ ref('stg_ef3__locations') }}
),
dim_school as (
    select * from {{ ref('dim_school') }}
),
formatted as (
    select 
        locations.k_location as k_classroom,
        dim_school.k_school,
        locations.tenant_code,
        locations.classroom_id_code,
        locations.maximum_seating,
        locations.optimum_seating
        
        -- custom data sources columns
        {{ add_cds_columns(cds_model_config=custom_data_sources_name) }}
    from locations
    join dim_school
        on locations.k_school = dim_school.k_school

    -- custom data sources
    {{ add_cds_joins_v1(cds_model_config=custom_data_sources_name, driving_alias='locations', join_cols=['k_location']) }}
    {{ add_cds_joins_v2(cds_model_config=custom_data_sources_name) }}
)
select * from formatted
order by tenant_code, k_school, k_classroom