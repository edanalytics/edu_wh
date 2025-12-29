with stg_candidate_staff_association as (
    select * from {{ ref('stg_tpdm__candidate_relationship_to_staff_associations') }}
),
dim_candidate as (
    select * from {{ ref('dim_candidate') }}
),
dim_staff as (
    select * from {{ ref('dim_staff') }}
),
{# TODO: should we combine this with implicit associations via staff & candidate personReferences? 
Bc of the way Ed-Fi defines people, this requires that the staff & candidate use the same person Identifier and Source System, else are treated as different people. #}
{# inner joins to enforce referential integrity, candidates and staff must exist #}
joined as (
    select
        stg_candidate_staff_association.k_candidate,
        stg_candidate_staff_association.k_staff,
        stg_candidate_staff_association.tenant_code,
        stg_candidate_staff_association.api_year,
        stg_candidate_staff_association.begin_date,
        stg_candidate_staff_association.staff_to_candidate_relationship
        {{ edu_edfi_source.extract_extension(model_name='stg_tpdm__candidate_relationship_to_staff_associations', flatten=False) }}
    from stg_candidate_staff_association
    join dim_candidate 
      on stg_candidate_staff_association.k_candidate = dim_candidate.k_candidate
    join dim_staff 
      on stg_candidate_staff_association.k_staff = dim_staff.k_staff
)
select * from joined
