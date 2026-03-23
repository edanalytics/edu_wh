{{
  config(
    post_hook=[
        "alter table {{ this }} alter column k_educator_preparation_program set not null",
        "alter table {{ this }} add primary key (k_educator_preparation_program)",
    ]
  )
}}

with stg_epp as (
    select * from {{ ref('stg_tpdm__educator_preparation_programs') }}
),

formatted as (
    select
        stg_epp.k_educator_preparation_program,
        stg_epp.tenant_code,
        stg_epp.program_id,
        stg_epp.ed_org_id,
        stg_epp.ed_org_type,
        stg_epp.program_type,
        stg_epp.program_name,
        stg_epp.accreditation_status,
        stg_epp.v_grade_levels
        {{ edu_edfi_source.extract_extension(model_name='stg_tpdm__educator_preparation_programs', flatten=False) }}
    from stg_epp
)
select * from formatted
