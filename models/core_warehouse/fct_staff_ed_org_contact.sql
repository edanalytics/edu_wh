-- SPDX-FileCopyrightText: 2022 Education Analytics
-- SPDX-License-Identifier: MPL-2.0

{{
  config(
    post_hook=[
        "alter table {{ this }} alter column k_staff_ed_org_contact set not null",
        "alter table {{ this }} add primary key (k_staff_ed_org_contact)",
        "alter table {{ this }} alter column k_staff set not null",
        "alter table {{ this }} alter column contact_title set not null",
        "alter table {{ this }} alter column email_address set not null"
        "alter table {{ this }} add constraint fk__{{ this.name }}__staff foreign key (k_staff) references {{ ref('dim_staff') }}",
        "alter table {{ this }} add constraint fk__{{ this.name }}__lea foreign key (k_lea) references {{ ref('dim_lea') }}",
        "alter table {{ this }} add constraint fk__{{ this.name }}__school foreign key (k_school) references {{ ref('dim_school') }}"
    ]
  )
}}

/* Unlike other Ed-Fi resources that support an array of addresses, this
   resource currently supports a single address. Because the address does not
   introduce a different grain, include its attributes directly on this fact. */

with stg_staff_edorg_contacts as (
    select * from {{ ref('stg_ef3__staff_education_organization_contact_associations') }}
),
stg_staff_edorg_contact_addresses as (
    select * from {{ ref('stg_ef3__staff_education_organization_contact_associations__addresses') }}
),
dim_staff as (
    select * from {{ ref('dim_staff') }}
),
formatted as (
    select 
        stg_staff_edorg_contacts.k_staff_ed_org_contact,
        stg_staff_edorg_contacts.k_staff,
        stg_staff_edorg_contacts.k_lea,
        stg_staff_edorg_contacts.k_school,
        stg_staff_edorg_contacts.tenant_code,
        stg_staff_edorg_contacts.school_year,
        stg_staff_edorg_contacts.contact_title,
        stg_staff_edorg_contacts.contact_type,
        stg_staff_edorg_contacts.email_address,
        stg_staff_edorg_contact_addresses.address_type,
        stg_staff_edorg_contact_addresses.street_address,
        stg_staff_edorg_contact_addresses.apartment_room_suite_number,
        stg_staff_edorg_contact_addresses.city,
        stg_staff_edorg_contact_addresses.state_code,
        stg_staff_edorg_contact_addresses.postal_code,
        stg_staff_edorg_contact_addresses.name_of_county,
        stg_staff_edorg_contact_addresses.building_site_number,
        stg_staff_edorg_contact_addresses.locale,
        stg_staff_edorg_contact_addresses.congressional_district,
        stg_staff_edorg_contact_addresses.county_fips_code,
        stg_staff_edorg_contact_addresses.do_not_publish,
        stg_staff_edorg_contact_addresses.latitude,
        stg_staff_edorg_contact_addresses.longitude,
        stg_staff_edorg_contact_addresses.full_address
    from stg_staff_edorg_contacts stg_staff_edorg_contacts
    left join stg_staff_edorg_contact_addresses stg_staff_edorg_contact_addresses
        on stg_staff_edorg_contact_addresses.k_staff_ed_org_contact = stg_staff_edorg_contacts.k_staff_ed_org_contact
    join dim_staff dim_staff
        on stg_staff_edorg_contacts.k_staff = dim_staff.k_staff
)
select * from formatted