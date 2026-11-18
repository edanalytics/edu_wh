-- SPDX-FileCopyrightText: 2022 Education Analytics
-- SPDX-License-Identifier: MPL-2.0

{{
  config(
    post_hook=[
        "alter table {{ this }} alter column k_staff_ed_org_contact_address_period set not null",
        "alter table {{ this }} add primary key (k_staff_ed_org_contact_address_period)",
        "alter table {{ this }} alter column k_staff_ed_org_contact set not null"
    ]
  )
}}

/* This table purposefully make sure that even addresses without any defined periods shows up.
   That way someone who inner joins to the table to get periods won't have addresses drop. */

with stg_staff_edorg_contact_addresses as (
    select * from {{ ref('stg_ef3__staff_education_organization_contact_associations__addresses') }}
),
formatted as (
    select 
        {{ dbt_utils.generate_surrogate_key(
            [
                'stg_staff_edorg_contact_addresses.k_staff_ed_org_contact',
                'stg_staff_edorg_contact_addresses.address_begin_date'
            ]
        ) }} as k_staff_ed_org_contact_address_period,
        stg_staff_edorg_contact_addresses.k_staff_ed_org_contact,
        stg_staff_edorg_contact_addresses.address_begin_date,
        stg_staff_edorg_contact_addresses.address_end_date
    from stg_staff_edorg_contact_addresses stg_staff_edorg_contact_addresses
)
select * from formatted