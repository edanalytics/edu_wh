{{
  config(
    post_hook=[
        "alter table {{ this }} alter column k_session set not null",
        "alter table {{ this }} add primary key (k_session)",
        "alter table {{ this }} add constraint fk_{{ this.name }}_school foreign key (k_school) references {{ ref('dim_school') }}",
    ]
  )
}}

with stg_sessions as (
    select * from {{ ref('stg_ef3__sessions') }}
),
dim_school as (
    select * from {{ ref('dim_school') }}
),
formatted as (
    select 
        stg_sessions.k_session,
        stg_sessions.k_school,
        stg_sessions.tenant_code,
        stg_sessions.school_year,
        stg_sessions.session_name,
        stg_sessions.session_begin_date,
        stg_sessions.session_end_date,
        stg_sessions.total_instructional_days,
        stg_sessions.academic_term
    from stg_sessions
    join dim_school
        on stg_sessions.k_school = dim_school.k_school
)
select * from formatted
order by tenant_code, k_school, session_begin_date desc