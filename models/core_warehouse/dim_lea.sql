{{
  config(
    post_hook=[
        "alter table {{ this }} alter column k_lea set not null",
        "alter table {{ this }} add primary key (k_lea)",
    ]
  )
}}

{{ cds_depends_on('edu:lea:custom_data_sources') }}
{% set custom_data_sources = var('edu:lea:custom_data_sources', []) %}

with stg_lea as (
    select * from {{ ref('stg_ef3__local_education_agencies') }}
),
tenant_lea_ownership as (
    select * from {{ ref('bld_ef3__tenant_lea_ownership') }}
),
choose_address as (
    {{ row_pluck(ref('stg_ef3__local_education_agencies__addresses'),
                key='k_lea',
                column='address_type',
                preferred='Physical',
                where='address_end_date is null') }}
),
formatted as (
    select 
        stg_lea.k_lea,
        stg_lea.k_lea__parent,
        stg_lea.k_sea,
        {% if var('src:ed_orgs:esc:enabled', True) %}
        stg_lea.k_esc,
        {% endif %}
        stg_lea.tenant_code,
        stg_lea.lea_id,
        stg_lea.lea_name,
        stg_lea.lea_short_name,
        stg_lea.parent_lea_id,
        stg_lea.lea_category,
        stg_lea.sea_id,
        stg_lea.esc_id,
        stg_lea.operational_status,
        stg_lea.charter_status,
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
        {{ add_cds_columns(custom_data_sources=custom_data_sources) }}
    from stg_lea
    left join choose_address 
        on stg_lea.k_lea = choose_address.k_lea
    join tenant_lea_ownership
        on stg_lea.tenant_code = tenant_lea_ownership.tenant_code
        and stg_lea.lea_id = cast(tenant_lea_ownership.lea_id as int)

    -- custom data sources
    {{ add_cds_joins_v1(custom_data_sources=custom_data_sources, driving_alias='stg_lea', join_cols=['k_lea']) }}
    {{ add_cds_joins_v2(custom_data_sources=custom_data_sources) }}
)
select * from formatted
order by tenant_code, k_lea
