{{
  config(
    post_hook=[
        "alter table {{ this }} alter column k_network set not null",
        "alter table {{ this }} add primary key (k_network)",
    ]
  )
}}

{% set custom_data_sources_name = "edu:network:custom_data_sources" %}

with stg_networks as (
    select * from {{ ref('stg_ef3__education_organization_networks') }}
),
formatted as (
    select
        stg_networks.k_network,
        stg_networks.tenant_code,
        stg_networks.network_id,
        stg_networks.network_name,
        stg_networks.network_purpose,
        stg_networks.operational_status,
        stg_networks.network_short_name,
        stg_networks.website

        -- custom data sources columns
        {{ add_cds_columns(cds_model_config=custom_data_sources_name) }}
    from stg_networks

    -- custom data sources
    {{ add_cds_joins_v1(cds_model_config=custom_data_sources_name, driving_alias='stg_networks', join_cols=['k_network']) }}
    {{ add_cds_joins_v2(cds_model_config=custom_data_sources_name) }}
)
select * from formatted
order by tenant_code, k_network