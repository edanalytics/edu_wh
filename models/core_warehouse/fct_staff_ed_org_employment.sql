-- SPDX-FileCopyrightText: 2022 Education Analytics
-- SPDX-License-Identifier: MPL-2.0

{{
  config(
    post_hook=[
        "alter table {{ this }} alter column k_staff_ed_org_employment set not null",
        "alter table {{ this }} add primary key (k_staff_ed_org_employment)",
        "alter table {{ this }} add constraint fk__{{ this.name }}__staff foreign key (k_staff) references {{ ref('dim_staff') }}",
        "alter table {{ this }} add constraint fk__{{ this.name }}__lea foreign key (k_lea) references {{ ref('dim_lea') }}",
        "alter table {{ this }} add constraint fk__{{ this.name }}__school foreign key (k_school) references {{ ref('dim_school') }}"
    ]
  )
}}

with stg_staff_edorg_employment as (
    select * from {{ ref('stg_ef3__staff_education_organization_employment_associations') }}
),
dim_staff as (
    select * from {{ ref('dim_staff') }}
),
formatted as (
    select 
        staff_edorg_employment.k_staff_ed_org_employment, 
        staff_edorg_employment.k_staff,
        staff_edorg_employment.k_lea,
        staff_edorg_employment.k_school,
        staff_edorg_employment.tenant_code,
        staff_edorg_employment.school_year,
        staff_edorg_employment.employment_status,
        staff_edorg_employment.credential_identifier,
        staff_edorg_employment.credential_state,
        staff_edorg_employment.department,
        staff_edorg_employment.hire_date,
        staff_edorg_employment.end_date,
        staff_edorg_employment.full_time_equivalency,
        staff_edorg_employment.hourly_wage,
        staff_edorg_employment.annual_wage,
        staff_edorg_employment.offer_date,
        staff_edorg_employment.separation,
        staff_edorg_employment.separation_reason
    from stg_staff_edorg_employment staff_edorg_employment
    join dim_staff dim_staff
        on staff_edorg_employment.k_staff = dim_staff.k_staff
)
select * from formatted