{{
  config(
    post_hook=[
        "alter table {{ this }} add primary key (k_school)",
    ]
  )
}}

with stg_school as (
    select * from {{ ref('stg_ef3__schools') }}
),
bld_ef3__wide_ids_school as (
    select * from {{ ref('bld_ef3__wide_ids_school') }}
),
bld_network_associations as (
  select * from {{ ref('bld_ef3__wide_school_network_assoc')}}
),
choose_address as (
    {{ row_pluck(ref('stg_ef3__schools__addresses'),
                key='k_school',
                column='address_type',
                preferred='Physical',
                where='address_end_date is null') }}
),
dim_lea as (
    select * from {{ ref('dim_lea') }}
),
formatted as (
    select 
        stg_school.k_school,
        stg_school.k_lea,
        -- if there are any networks configured for school assoc, add those foreign keys as k_network__{network_type}
        {% set network_types = dbt_utils.get_column_values(
                           table=ref('xwalk_network_association_types'),
                           column='network_type',
                           where="association_type = 'school'",
                           order_by='network_type')
        %}
        {%- if network_types is not none and network_types | length -%}
          {%- for network_type in network_types -%}
            bld_network_associations.k_network__{{network_type}},
          {%- endfor -%}
        {%- endif %}
        stg_school.tenant_code,
        stg_school.school_id,
        {{ accordion_columns(
            source_table='bld_ef3__wide_ids_school',
            exclude_columns=['tenant_code', 'k_school']) }}
        stg_school.school_name,
        stg_school.school_short_name,
        dim_lea.lea_name,
        dim_lea.lea_id,
        stg_school.school_category,
        stg_school.school_type,
        stg_school.operational_status,
        stg_school.administrative_funding_control,
        stg_school.internet_access,
        stg_school.title_i_part_a_school_designation,
        stg_school.charter_status,
        stg_school.charter_approval_agency,
        stg_school.magnet_type,

        -- custom indicators
        {% set custom_indicators = var('edu:schools:custom_indicators') %}
        {%- if custom_indicators is not none and custom_indicators | length -%}
          {%- for indicator in custom_indicators -%}
              {{ custom_indicators[indicator]['where'] }} as {{ indicator }},
          {%- endfor -%}
        {%- endif %}

        stg_school.website,
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
    from stg_school
    join dim_lea 
        on stg_school.k_lea = dim_lea.k_lea
    left join bld_ef3__wide_ids_school
        on stg_school.k_school = bld_ef3__wide_ids_school.k_school
    left join choose_address
        on stg_school.k_school = choose_address.k_school
    left join bld_network_associations
        on stg_school.k_school = bld_network_associations.k_school
)
select * from formatted
order by tenant_code, k_school
