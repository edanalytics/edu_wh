{{
  config(
    post_hook=[
        "alter table {{ this }} alter column k_network set not null",
        "alter table {{ this }} add primary key (k_network)",
    ]
  )
}}

{{ cds_depends_on('edu:network:custom_data_sources') }}
{% set custom_data_sources = var('edu:network:custom_data_sources', []) %}

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
        {{ add_cds_columns(custom_data_sources=custom_data_sources) }}
    from stg_networks

    -- custom data sources
    {{ add_cds_joins_v1(custom_data_sources=custom_data_sources, driving_alias='stg_networks', join_cols=['k_network']) }}
    {{ add_cds_joins_v2(custom_data_sources=custom_data_sources) }}
)
select * from formatted
order by tenant_code, k_network