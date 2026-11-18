-- SPDX-FileCopyrightText: 2022 Education Analytics
-- SPDX-License-Identifier: MPL-2.0

{{
  config(
    post_hook=[
        "alter table {{ this }} alter column k_staff_ed_org_assignment set not null",
        "alter table {{ this }} add primary key (k_staff_ed_org_assignment)",
        "alter table {{ this }} add constraint fk__{{ this.name }}__staff foreign key (k_staff) references {{ ref('dim_staff') }}",
        "alter table {{ this }} add constraint fk__{{ this.name }}__lea foreign key (k_lea) references {{ ref('dim_lea') }}",
        "alter table {{ this }} add constraint fk__{{ this.name }}__school foreign key (k_school) references {{ ref('dim_school') }}",
        "alter table {{ this }} add constraint fk__{{ this.name }}__employment foreign key (k_staff_ed_org_employment) references {{ ref('fct_staff_ed_org_employment') }}"
    ]
  )
}}

with stg_staff_ed_org_assign as (
    select *,
        staff_ed_org_employment_reference:staffUniqueId::string as staff_edorg_employment_reference_staff_unique_id,
        staff_ed_org_employment_reference:educationOrganizationId::int as staff_edorg_employment_reference_ed_org_id,
        staff_ed_org_employment_reference:hireDate::date as staff_edorg_employment_reference_hire_date,
        {{ edu_edfi_source.extract_descriptor('staff_ed_org_employment_reference:employmentStatusDescriptor::string') }} as staff_edorg_employment_reference_employment_status
    from {{ ref('stg_ef3__staff_education_organization_assignment_associations')}}
),
dim_staff as (
    select * from {{ ref('dim_staff') }}
),
formatted as (
    select
        stg_staff_ed_org_assign.k_staff_ed_org_assignment,
        dim_staff.k_staff,
        stg_staff_ed_org_assign.k_lea,
        stg_staff_ed_org_assign.k_school,
        stg_staff_ed_org_assign.tenant_code,
        stg_staff_ed_org_assign.school_year,
        case
            when stg_staff_ed_org_assign.staff_ed_org_employment_reference is not null then 
                {{ dbt_utils.generate_surrogate_key(
                    [
                        'stg_staff_ed_org_assign.tenant_code',
                        'stg_staff_ed_org_assign.api_year',
                        'stg_staff_ed_org_assign.staff_edorg_employment_reference_ed_org_id',
                        'stg_staff_ed_org_assign.staff_edorg_employment_reference_employment_status',
                        'stg_staff_ed_org_assign.staff_edorg_employment_reference_hire_date',
                        'stg_staff_ed_org_assign.staff_edorg_employment_reference_staff_unique_id'
                    ]
                ) }}
        end as k_staff_ed_org_employment,
        stg_staff_ed_org_assign.position_title,
        stg_staff_ed_org_assign.begin_date,
        stg_staff_ed_org_assign.end_date,
        stg_staff_ed_org_assign.full_time_equivalency,
        stg_staff_ed_org_assign.order_of_assignment,
        stg_staff_ed_org_assign.credential_identifier,
        stg_staff_ed_org_assign.credential_state,
        stg_staff_ed_org_assign.staff_classification
        {{ edu_edfi_source.extract_extension(model_name='stg_ef3__staff_education_organization_assignment_associations', flatten=False) }}
    from stg_staff_ed_org_assign
    join dim_staff
        on stg_staff_ed_org_assign.k_staff = dim_staff.k_staff
),
check_active as (
    /* I don't think this active check is good. I think it will have the same sorts of problems 
    that the original enrollment checks had. */
    select
        *,
        iff(
            school_year = max(school_year)
                over(partition by tenant_code)
            and (end_date is null
                or end_date >= current_date())
            and begin_date <= current_date(),
            true, false
        ) as is_active_assignment
    from formatted
)
select * from check_active