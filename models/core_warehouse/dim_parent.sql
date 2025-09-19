{{
  config(
    post_hook=[
        "alter table {{ this }} alter column k_parent set not null",
        "alter table {{ this }} add primary key (k_parent)",
    ]
  )
}}

{{ cds_depends_on('edu:parent:custom_data_sources') }}
{% set custom_data_sources = var('edu:parent:custom_data_sources', []) %}

with stg_parent as (
    -- parents were renamed to contacts in Data Standard v5.0
    -- the contacts staging tables contain both parent and contact records
    select * from {{ ref('stg_ef3__contacts') }}
),
parent_phones_wide as (
    select * from {{ ref('bld_ef3__parent_wide_phone_numbers') }}
),
parent_emails_wide as (
    select * from {{ ref('bld_ef3__parent_wide_emails') }}
),
choose_address as (
    {{ row_pluck(ref('stg_ef3__contacts__addresses'),
                key='k_contact',
                column='address_type',
                preferred='Home',
                where='(do_not_publish is null or not do_not_publish)') }}
),
formatted as (
    select 
        stg_parent.k_contact as k_parent,
        stg_parent.tenant_code,
        stg_parent.api_year as school_year,
        stg_parent.contact_unique_id as parent_unique_id,
        stg_parent.person_id,
        stg_parent.login_id,
        stg_parent.person_source_system,
        stg_parent.last_name || ', ' || stg_parent.first_name as display_name,
        stg_parent.first_name,
        stg_parent.last_name,
        stg_parent.middle_name,
        stg_parent.maiden_name,
        stg_parent.personal_title_prefix,
        stg_parent.generation_code_suffix,
        stg_parent.preferred_first_name,
        stg_parent.preferred_last_name,
        stg_parent.gender_identity,
        stg_parent.sex,
        stg_parent.highest_completed_level_of_education,
        {{ accordion_columns(
            source_table='bld_ef3__parent_wide_phone_numbers',
            exclude_columns=["k_parent", "tenant_code"],
            source_alias='parent_phones_wide'
        ) }}
        {{ accordion_columns(
            source_table='bld_ef3__parent_wide_emails',
            exclude_columns=["k_parent", "tenant_code"],
            source_alias='parent_emails_wide'
        ) }}
        choose_address.full_address

        -- custom data sources columns
        {{ add_cds_columns(custom_data_sources=custom_data_sources) }}
    from stg_parent
    left join parent_phones_wide
      on stg_parent.k_contact = parent_phones_wide.k_parent --k_contact has been renamed back to k_parent in the build models
    left join parent_emails_wide
      on stg_parent.k_contact = parent_emails_wide.k_parent
    left join choose_address
        on stg_parent.k_contact = choose_address.k_contact

    -- custom data sources
    {{ add_cds_joins_v1(custom_data_sources=custom_data_sources, driving_alias='stg_parent', join_cols=['k_contact']) }}
    {{ add_cds_joins_v2(custom_data_sources=custom_data_sources) }}
)
select * from formatted
order by tenant_code, k_parent