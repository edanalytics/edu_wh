with fct_staff_school as (
    select * from {{ ref('fct_staff_school_association') }}
),
stg_staff_ed_org_assign as (
    select * from {{ ref('stg_ef3__staff_education_organization_assignment_associations')}}
),
staff_access_patterns as (
    select * from {{ ref('staff_classification_access') }}
),
dim_staff as (
    select * from {{ ref('dim_staff') }}
),
dim_school as (
    select * from {{ ref('dim_school') }}
),
role_based_access_school as (
    select 
        fct_staff_school.k_staff,
        dim_staff.email_address,
        dim_staff.tenant_code,
        staff_access_patterns.access_level,
        fct_staff_school.position_title as role_name,
        dim_school.k_lea,
        dim_school.k_school
    from fct_staff_school
    join dim_staff
        on fct_staff_school.k_staff = dim_staff.k_staff
    join staff_access_patterns
        -- note: compare position_title and staff_classification
        on fct_staff_school.position_title = staff_access_patterns.staff_classification
    join dim_school 
        on fct_staff_school.k_school = dim_school.k_school
    where fct_staff_school.is_active_assignment
    and staff_access_patterns.access_level = 'school'
),
role_based_access_district as (
    select 
        stg_staff_ed_org_assign.k_staff,
        dim_staff.email_address,
        dim_staff.tenant_code,
        staff_access_patterns.access_level,
        stg_staff_ed_org_assign.staff_classification as role_name,
        dim_school.k_lea,
        dim_school.k_school
    from stg_staff_ed_org_assign
    join dim_staff 
        on stg_staff_ed_org_assign.k_staff = dim_staff.k_staff
    join staff_access_patterns
        on stg_staff_ed_org_assign.staff_classification = staff_access_patterns.staff_classification
    join dim_school 
        on stg_staff_ed_org_assign.k_lea = dim_school.k_lea
    where stg_staff_ed_org_assign.ed_org_type = 'LocalEducationAgency'
    and staff_access_patterns.access_level = 'district'
),
{# staff_access_table as (
    select 
        distinct 
        fct_staff_school.k_staff,
        dim_staff.email_address,
        dim_staff.tenant_code,
        dim_school.k_lea,
        dim_school.k_school
    from fct_staff_school
    join dim_staff
        on fct_staff_school.k_staff = dim_staff.k_staff
    join staff_access_patterns
        on fct_staff_school.staff_classification = staff_access_patterns.staff_classification
    join dim_school 
        on fct_staff_school.k_school = dim_school.k_school
    where fct_staff_school.is_active_assignment
    and staff_access_patterns.access_level in ('school', 'district') #}
-- non-edfi derived access
direct_access as (
    select * from {{ ref('direct_user_access') }}
),
direct_access_table as (
    select 
        null as k_staff,
        lower(direct_access.email_address::string) as email_address,
        direct_access.tenant_code::string,
        direct_access.access_level,
        direct_access.role_name,
        dim_school.k_lea,
        dim_school.k_school
    from direct_access
    join dim_school 
        on direct_access.tenant_code = dim_school.tenant_code
        and direct_access.lea_id     = dim_school.lea_id
        and (direct_access.school_id::int = dim_school.school_id
            or (direct_access.access_level = 'district'
            and direct_access.school_id is null))
),
stacked as (
    select * from role_based_access_district
    union all
    select * from role_based_access_school
    union all
    select * from direct_access_table
),
deduped as (
    {{ dbt_utils.deduplicate(
        relation='stacked',
        partition_by='email_address, k_school',
        order_by='role_name'
    ) }}
)
select *
from deduped
order by k_lea, k_school, email_address