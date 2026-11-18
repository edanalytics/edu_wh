{{
  config(
    post_hook=[
        "alter table {{ this }} alter column k_post_secondary_institution set not null",
        "alter table {{ this }} add primary key (k_post_secondary_institution)",
    ]
  )
}}

with stg_psi as (
    select * from {{ ref('stg_ef3__post_secondary_institutions') }}
),
choose_address as (
    {{ edu_wh.row_pluck(ref('stg_ef3__post_secondary_institutions__addresses'),
        key='k_post_secondary_institution',
        column='address_type',
        preferred='Physical',
        where='address_end_date is null') }}
),
formatted as (
    select 
        stg_psi.k_post_secondary_institution,
        stg_psi.tenant_code,
        stg_psi.post_secondary_institution_id as psi_id,
        stg_psi.name_of_institution as psi_name,
        stg_psi.short_name_of_institution as psi_short_name,
        stg_psi.administrative_funding_control,
        stg_psi.post_secondary_institution_level,
        stg_psi.operational_status_descriptor as operational_status,
        stg_psi.web_site,
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
    from stg_psi
    left join choose_address 
        on stg_psi.k_post_secondary_institution = choose_address.k_post_secondary_institution
)
select * from formatted
order by tenant_code, k_post_secondary_institution
