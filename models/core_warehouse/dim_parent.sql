{{
  config(
    post_hook=[
        "alter table {{ this }} add primary key (k_parent)",
    ]
  )
}}

with stg_parent as (
    select * from {{ ref('stg_ef3__parents') }}
),
formatted as (
    select 
        stg_parent.k_parent,
        stg_parent.tenant_code,
        stg_parent.parent_unique_id,
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
        stg_parent.sex,
        stg_parent.highest_completed_level_of_education
        -- leaving out contact info/addresses entirely given the difference in grain
        -- todo: need to determine what to do here
    from stg_parent
)
select * from formatted
order by tenant_code, k_parent