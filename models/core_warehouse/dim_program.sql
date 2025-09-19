{{
  config(
    post_hook=[
        "alter table {{ this }} alter column k_program set not null",
        "alter table {{ this }} add primary key (k_program)",
    ]
  )
}}

{{ cds_depends_on('edu:program:custom_data_sources') }}
{% set custom_data_sources = var('edu:program:custom_data_sources', []) %}

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

        -- custom data sources columns
        {{ add_cds_columns(custom_data_sources=custom_data_sources) }}
    from stg_programs

    -- custom data sources
    {{ add_cds_joins_v1(custom_data_sources=custom_data_sources, driving_alias='stg_programs', join_cols=['k_program']) }}
    {{ add_cds_joins_v2(custom_data_sources=custom_data_sources) }}
)

select * from formatted