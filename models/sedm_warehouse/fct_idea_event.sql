{{
  config(
    post_hook=[
        "alter table {{ this }} add primary key (k_idea_event)",
        "alter table {{ this }} add constraint fk_{{ this.name }}_student foreign key (k_student) references {{ ref('dim_student') }}",
        "alter table {{ this }} add constraint fk_{{ this.name }}_student_xyear foreign key (k_student_xyear) references {{ ref('dim_student') }}"
    ]
  )
}}

with stg_idea_events as (
    select * from {{ ref('stg_sedm__idea_events') }}
),
formatted as (
    select
        stg_idea_events.k_idea_event,
        stg_idea_events.k_student,
        stg_idea_events.k_student_xyear,
        stg_idea_events.tenant_code,
        stg_idea_events.idea_event_id,
        stg_idea_events.idea_event,
        stg_idea_events.event_begin_date,
        stg_idea_events.event_end_date,
        stg_idea_events.event_narrative,
        stg_idea_events.event_reason,
        stg_idea_events.event_compliance
    from stg_idea_events
)
select * from formatted