{{
  config(
    post_hook=[
        "alter table {{ this }} alter column k_staff_educaton_organization_assignment_association set not null",
        "alter table {{ this }} add primary key (k_staff_educaton_organization_assignment_associatio)",
        "alter table {{ this }} add constraint fk_{{ this.name }}_staff foreign key (k_staff) references {{ ref('dim_staff') }}",
    ]
  )
}}

with stg_staff_ed_org_assign as (
    select * from {{ ref('stg_ef3__staff_education_organization_assignment_associations')}}
),

dim_school as (
    select * from {{ ref('dim_school') }}
),
dim_staff as (
    select * from {{ ref('dim_staff') }}
),
dim_school_calendar as (
    select * from {{ ref('dim_school_calendar') }}
),
formatted as (
    select 
        dim_staff.k_staff,
        stg_staff_ed_org_assign.k_lea,
        stg_staff_ed_org_assign.k_school,
        stg_staff_school.tenant_code,
        stg_staff_school.school_year,
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
    join dim_school
        on stg_staff_ed_org_assign.k_school = dim_school.k_school
    join dim_staff
        on stg_staff_ed_org_assign.k_staff = dim_staff.k_staff
),
check_active as (
    select
        {{ dbt_utils.generate_surrogate_key(
            [
                'tenant_code',
                'school_year',
                'begin_date',
                'k_staff',
                'staff_classification'
            ]
        )}} as k_staff_educaton_organization_assignment_association
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