{{
  config(
    post_hook=[
        "alter table {{ this }} add primary key (k_network, k_ed_org, ed_org_type)",
        "alter table {{ this }} add constraint fk_{{ this.name }}_network foreign key (k_network) references {{ ref('dim_network') }}",
    ]
  )
}}{# no foreign key constraint for k_ed_org because it could be a key to dim_school or dim_lea #}

with stg_ed_org_network as (
    select * from {{ ref('stg_ef3__education_organization_network_associations') }}
),
dim_network as (
    select * from {{ ref('dim_network') }}
),

formatted as (
    select 
        dim_network.k_network,
        case 
            when education_organization_reference:link:rel::string  = 'School'
            then stg_ed_org_network.k_school
            when education_organization_reference:link:rel::string  = 'LocalEducationAgency'
            then stg_ed_org_network.k_lea
        end as k_ed_org,
        case 
            when education_organization_reference:link:rel::string  = 'School'
            then 'school'
            when education_organization_reference:link:rel::string  = 'LocalEducationAgency'
            then 'lea'
            else null
        end as ed_org_type,
        stg_ed_org_network.tenant_code,
        stg_ed_org_network.network_id,
        stg_ed_org_network.ed_org_id,
        stg_ed_org_network.begin_date,
        stg_ed_org_network.end_date,
        -- create indicator for active association
        iff(
            -- association has not ended
            (end_date is null
                or end_date >= current_date())
            -- association has begun
            and begin_date <= current_date(),
            true, false
        ) as is_active_association
    from stg_ed_org_network
    join dim_network
        on stg_ed_org_network.k_network = dim_network.k_network
    where true
    -- exclude associations that ended before they started
    and (end_date >= begin_date
        or end_date is null)
)
select * from formatted
