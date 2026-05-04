{{
  config(
    post_hook=[
        "alter table {{ this }} alter column k_idea_event set not null",
        "alter table {{ this }} alter column k_student set not null",
        "alter table {{ this }} alter column k_student_xyear set not null",
        "alter table {{ this }} add primary key (k_idea_event)",
        "alter table {{ this }} add constraint fk_{{ this.name }}_student foreign key (k_student) references {{ ref('dim_student') }}",
    ]
  )
}}

with stage as (
    select * from {{ ref('stg_sedm__idea_events') }}
),

dim_student as (
    select * from {{ ref('dim_student') }}
),

formatted as (
    select
        stage.k_idea_event,
        dim_student.k_student,
        dim_student.k_student_xyear,
        stage.tenant_code,
        stage.school_year,
        stage.idea_event_id,
        stage.ed_org_id,
        stage.ed_org_type,
        stage.idea_event,
        stage.event_begin_date,
        stage.event_end_date,
        stage.event_narrative,
        stage.event_reason,
        stage.event_compliance
        {{ edu_edfi_source.extract_extension(model_name='stg_sedm__idea_events', flatten=False) }}
    from stage
        inner join dim_student
            on stage.k_student = dim_student.k_student
)

select * from formatted
