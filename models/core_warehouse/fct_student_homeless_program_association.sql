{{
  config(
    post_hook=[
        "alter table {{ this }} alter column k_student_program set not null",
        "alter table {{ this }} alter column k_student set not null",
        "alter table {{ this }} alter column k_student_xyear set not null",
        "alter table {{ this }} alter column k_program set not null",
        "alter table {{ this }} alter column program_enroll_begin_date set not null",
        "alter table {{ this }} alter column ed_org_id set not null",
        "alter table {{ this }} add primary key (k_student_program)",
        "alter table {{ this }} add constraint fk_{{ this.name }}_student foreign key (k_student) references {{ ref('dim_student') }}",
        "alter table {{ this }} add constraint fk_{{ this.name }}_program foreign key (k_program) references {{ ref('dim_program') }}",
    ]
  )
}}

{{ cds_depends_on('edu:student_homeless_program_association:custom_data_sources') }}
{% set custom_data_sources = var('edu:student_homeless_program_association:custom_data_sources', []) %}

with stage as (
    select * from {{ ref('stg_ef3__student_homeless_program_associations') }}
),

dim_student as (
    select * from {{ ref('dim_student') }}
),

dim_program as (
    select * from {{ ref('dim_program') }}
),

formatted as (
    select
        stage.k_student_program,
        dim_student.k_student,
        dim_student.k_student_xyear,
        dim_program.k_program,
        dim_program.k_lea,
        dim_program.k_school,
        stage.ed_org_id,
        
        stage.tenant_code,
        dim_program.school_year,
        stage.program_enroll_begin_date,
        stage.program_enroll_end_date,

        stage.is_awaiting_foster_care,
        stage.is_homeless_unaccompanied_youth,
        stage.homeless_primary_nighttime_residence,

        stage.is_served_outside_regular_session,
        stage.participation_status,
        stage.participation_status_designated_by,
        stage.participation_status_begin_date,
        stage.participation_status_end_date,
        stage.reason_exited
        {# add any extension columns configured from stg_ef3__student_homeless_program_associations #}
        {{ edu_edfi_source.extract_extension(model_name='stg_ef3__student_homeless_program_associations', flatten=False) }}

        -- custom data sources columns
        {{ add_cds_columns(custom_data_sources=custom_data_sources) }}
    from stage

        inner join dim_student
            on stage.k_student = dim_student.k_student

        inner join dim_program
            on stage.k_program = dim_program.k_program

    -- custom data sources
    {{ add_cds_joins_v1(custom_data_sources=custom_data_sources, driving_alias='stage', join_cols=['k_student_program']) }}
    {{ add_cds_joins_v2(custom_data_sources=custom_data_sources) }}
)

select * from formatted
