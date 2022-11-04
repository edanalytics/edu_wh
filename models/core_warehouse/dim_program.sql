{{
  config(
    post_hook=[
        "alter table {{ this }} add primary key (k_program)",
    ]
  )
}}

with stg_programs as (
    select * from {{ ref('stg_ef3__programs') }}
),

formatted as (
    select
        stg_programs.k_program,
        stg_programs.k_lea,
        stg_programs.k_school,
        stg_programs.api_year as school_year,
        stg_programs.tenant_code,
        stg_programs.ed_org_id,
        stg_programs.ed_org_type,
        stg_programs.program_id,
        stg_programs.program_name,
        stg_programs.program_type
    from stg_programs
)

select * from formatted