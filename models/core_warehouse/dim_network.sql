{{
  config(
    post_hook=[
        "alter table {{ this }} add primary key (k_network)",
    ]
  )
}}


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
    from stg_networks
)
select * from formatted
order by tenant_code, k_network