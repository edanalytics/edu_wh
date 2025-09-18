{{
  config(
    post_hook=[
        "alter table {{ this }} alter column k_esc set not null",
        "alter table {{ this }} add primary key (k_esc)",
    ]
  )
}}

{% set custom_data_sources_name = "edu:esc:custom_data_sources" %}

with stg_esc as (
    select * from {{ ref('stg_ef3__education_service_centers') }}
),

choose_address as (
    {{ row_pluck(ref('stg_ef3__education_service_centers__addresses'),
                key='k_esc',
                column='address_type',
                preferred='Physical',
                where='address_end_date is null') }}
),
formatted as (
    select 
        stg_esc.k_esc,
        stg_esc.tenant_code,
        stg_esc.esc_id,
        stg_esc.esc_name,
        stg_esc.esc_short_name,
        stg_esc.sea_id,
        stg_esc.website,
        stg_esc.operational_status,
        choose_address.address_type,
        choose_address.street_address,
        choose_address.city,
        choose_address.name_of_county,
        choose_address.state_code,
        choose_address.postal_code,
        choose_address.building_site_number,
        choose_address.locale,
        choose_address.congressional_district,
        choose_address.county_fips_code,
        choose_address.latitude,
        choose_address.longitude
        
        -- custom data sources columns
        {{ add_cds_columns(cds_model_config=custom_data_sources_name) }}
    from stg_esc
    left join choose_address 
        on stg_esc.k_esc = choose_address.k_esc

    -- custom data sources
    {{ add_cds_joins_v1(cds_model_config=custom_data_sources_name, driving_alias='stg_esc', join_cols=['k_esc']) }}
    {{ add_cds_joins_v2(cds_model_config=custom_data_sources_name) }}
)
select * from formatted
order by tenant_code, k_esc