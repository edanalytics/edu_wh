{{
  config(
    post_hook=[
        "alter table {{ this }} alter column k_student set not null",
        "alter table {{ this }} alter column k_student_xyear set not null",
        "alter table {{ this }} alter column k_discipline_incident set not null",
        "alter table {{ this }} add primary key (k_student, k_student_xyear, k_discipline_incident)",
        "alter table {{ this }} add constraint fk_{{ this.name }}_student foreign key (k_student) references {{ ref('dim_student') }}",
        "alter table {{ this }} add constraint fk_{{ this.name }}_school foreign key (k_school) references {{ ref('dim_school') }}"
    ]
  )
}}

{% set custom_data_sources_name = "edu:student_discipline_incident_non_offenders:custom_data_sources" %}

with stg_stu_discipline_incident_non_offenders as (
    select * from {{ ref('stg_ef3__student_discipline_incident_non_offender_associations') }}
),
dim_student as (
    select * from {{ ref('dim_student') }}
),
dim_school as (
    select * from {{ ref('dim_school') }}
),
dim_discipline_incident as (
    select * from {{ ref('dim_discipline_incident') }}
),
participation_codes as (
    select distinct
        k_student, k_student_xyear, k_discipline_incident,
        array_agg(participation_code) over (
            partition by k_student, k_student_xyear, k_discipline_incident
            order by r rows between unbounded preceding and unbounded following) as participation_codes_array
    from (
            select
                k_student,
                k_student_xyear,
                k_discipline_incident,
                participation_code,
                row_number() over (partition by k_student, k_student_xyear, k_discipline_incident order by participation_code asc) as r
            from {{ ref('stg_ef3__student_discipline_incident_non_offender_associations__participation_codes') }}
    ) x
),
formatted as (
    select 
        dim_student.k_student,
        dim_student.k_student_xyear,
        dim_school.k_school,
        dim_discipline_incident.k_discipline_incident,
        stg_stu_discipline_incident_non_offenders.tenant_code,
        stg_stu_discipline_incident_non_offenders.school_id,
        stg_stu_discipline_incident_non_offenders.incident_id,
        false as is_offender,
        -- there is typically only a single value here, choosing the first option for analytical use cases
        participation_codes.participation_codes_array[0]::string as participation_code,
        participation_codes.participation_codes_array
        {# add any extension columns configured from stg_ef3__student_discipline_incident_non_offender_associations #}
        {{ edu_edfi_source.extract_extension(model_name='stg_ef3__student_discipline_incident_non_offender_associations', flatten=False) }}

        -- custom data sources columns
        {{ add_cds_columns(cds_model_config=custom_data_sources_name) }}
    from stg_stu_discipline_incident_non_offenders
    join participation_codes 
        on stg_stu_discipline_incident_non_offenders.k_student = participation_codes.k_student
        and stg_stu_discipline_incident_non_offenders.k_student_xyear = participation_codes.k_student_xyear
        and stg_stu_discipline_incident_non_offenders.k_discipline_incident = participation_codes.k_discipline_incident
    join dim_student on stg_stu_discipline_incident_non_offenders.k_student = dim_student.k_student
    join dim_school on stg_stu_discipline_incident_non_offenders.k_school = dim_school.k_school
    join dim_discipline_incident on stg_stu_discipline_incident_non_offenders.k_discipline_incident = dim_discipline_incident.k_discipline_incident
        
    -- custom data sources
    {{ add_cds_joins_v2(cds_model_config=custom_data_sources_name) }}
)
select *
from formatted