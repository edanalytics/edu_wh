{{
  config(
    post_hook=[
        "alter table {{ this }} add constraint fk_{{ this.name }}_student_iep foreign key (k_student_iep) references {{ ref('fct_student_iep') }}",
    ]
  )
}}

with disabilities as (
    select * from {{ ref('stg_sedm__student_iep_disability_collections') }}
),

formatted as (
    select
        disabilities.k_student_iep,
        disabilities.tenant_code,
        disabilities.school_year,
        disabilities.ed_org_id,
        disabilities.disability_descriptor,
        disabilities.disability_determination_source_type_descriptor,
        disabilities.disability_diagnosis,
        disabilities.order_of_disability
        {{ edu_edfi_source.extract_extension(model_name='stg_sedm__student_iep_disability_collections', flatten=False) }}
    from disabilities
)

select * from formatted
