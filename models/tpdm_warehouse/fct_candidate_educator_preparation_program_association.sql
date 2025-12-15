{{
  config(
    post_hook=[
        "alter table {{ this }} alter column k_candidate set not null",
        "alter table {{ this }} alter column k_educator_preparation_program set not null",
        "alter table {{ this }} alter column begin_date set not null",
        "alter table {{ this }} add primary key (k_candidate, k_educator_preparation_program, begin_date)",
        "alter table {{ this }} add constraint fk_{{ this.name }}_staff foreign key (k_candidate) references {{ ref('dim_candidate') }}",
        "alter table {{ this }} add constraint fk_{{ this.name }}_educator_preparation_program foreign key (k_educator_preparation_program) references {{ ref('dim_educator_preparation_program') }}",
    ]
  )
}}

with stage as (
    select * from {{ ref('stg_tpdm__candidate_educator_preparation_program_associations') }}
),

dim_candidate as (
    select * from {{ ref('dim_candidate') }}
),

dim_epp as (
    select * from {{ ref('dim_educator_preparation_program') }}
),

formatted as (
    select
        dim_candidate.k_candidate,
        dim_candidate.k_candidate_xyear,
        dim_epp.k_educator_preparation_program,
        stage.tenant_code,
        stage.school_year,
        stage.begin_date,
        stage.end_date,
        stage.program_type,
        stage.reason_exited,
        stage.epp_program_pathway,
        stage.v_degree_specializations,
        stage.v_cohort_years
    from stage
    join dim_epp
        on stage.k_educator_preparation_program = dim_epp.k_educator_preparation_program
    join dim_candidate
        on stage.k_candidate = dim_candidate.k_candidate
)
select * from formatted
