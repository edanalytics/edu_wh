{{
  config(
    post_hook=[
        "alter table {{ this }} add primary key (k_lea)",
    ]
  )
}}

with stg_lea as (
    select * from {{ ref('stg_ef3__local_education_agencies') }}
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
        stg_lea.tenant_code,
        stg_lea.lea_id,
        stg_lea.lea_name,
        stg_lea.lea_short_name,
        stg_lea.parent_lea_id,
        stg_lea.lea_category,
        stg_lea.education_service_center_id,
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
    from stg_lea
    left join choose_address 
        on stg_lea.k_lea = choose_address.k_lea
)
select * from formatted
order by tenant_code, k_lea