{{
  config(
    post_hook=[
        "alter table {{ this }} alter column k_student set not null",
        "alter table {{ this }} alter column k_student_xyear set not null",
        "alter table {{ this }} alter column k_discipline_incident set not null",
        "alter table {{ this }} alter column behavior_type set not null",
        "alter table {{ this }} add primary key (k_student, k_student_xyear, k_discipline_incident, behavior_type)",
        "alter table {{ this }} add constraint fk_{{ this.name }}_student foreign key (k_student) references {{ ref('dim_student') }}",
        "alter table {{ this }} add constraint fk_{{ this.name }}_school foreign key (k_school) references {{ ref('dim_school') }}",
        "alter table {{ this }} add constraint fk_{{ this.name }}_discipline_incident foreign key (k_discipline_incident) references {{ ref('dim_discipline_incident') }}"
    ]
  )
}}

{% set custom_data_sources_name = "edu:student_discipline_incident_behaviors:custom_data_sources" %}

with stg_stu_discipline_incident_behaviors as (
    select * from {{ ref('stg_ef3__student_discipline_incident_behavior_associations') }}
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
xwalk_discipline_behaviors as (
    select * from {{ ref('xwalk_discipline_behaviors') }}
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
            from {{ ref('stg_ef3__student_discipline_incident_behavior_associations__participation_codes') }}
    ) x
),
formatted as (
    select 
        dim_student.k_student,
        dim_student.k_student_xyear,
        dim_school.k_school,
        dim_discipline_incident.k_discipline_incident,
        -- add this for easier join back to discipline actions
        {{ dbt_utils.generate_surrogate_key(
            ['dim_student.k_student',
             'dim_discipline_incident.k_discipline_incident',
             'lower(stg_stu_discipline_incident_behaviors.behavior_type)']
        ) }} as k_student_discipline_incident_behavior,
        stg_stu_discipline_incident_behaviors.tenant_code,
        stg_stu_discipline_incident_behaviors.school_id,
        stg_stu_discipline_incident_behaviors.incident_id,
        stg_stu_discipline_incident_behaviors.behavior_type,
        stg_stu_discipline_incident_behaviors.behavior_detailed_description,
        true as is_offender,
        xwalk_discipline_behaviors.severity_order,
        -- for a specific discipline event (which can include multiple disciplines)
        -- flag the most severe discipline
        -- this will also handle if there are ties in severity and just choose the first option
        case
            when xwalk_discipline_behaviors.severity_order is null
                then null
            when xwalk_discipline_behaviors.severity_order is not null 
                and 1 = row_number() over (partition by dim_student.k_student, dim_student.k_student_xyear, dim_school.k_school, stg_stu_discipline_incident_behaviors.k_discipline_incident 
                                           order by xwalk_discipline_behaviors.severity_order desc nulls last)
                then true
            else false
        end as is_most_severe_behavior,
        -- bring in any additional custom columns from xwalk, does nothing if there are no extra columns
        {{ accordion_columns('xwalk_discipline_behaviors', exclude_columns=['behavior_type', 'severity_order']) }}
        -- there is typically only a single value here, choosing the first option for analytical use cases
        participation_codes.participation_codes_array[0]::string as participation_code,
        participation_codes.participation_codes_array
        {# add any extension columns configured from stg_ef3__student_discipline_incident_behavior_associations #}
        {{ edu_edfi_source.extract_extension(model_name='stg_ef3__student_discipline_incident_behavior_associations', flatten=False) }}

        -- custom data sources columns
        {{ add_cds_columns(cds_model_config=custom_data_sources_name) }}
    from stg_stu_discipline_incident_behaviors
    left join participation_codes 
        on stg_stu_discipline_incident_behaviors.k_student = participation_codes.k_student
        and stg_stu_discipline_incident_behaviors.k_student_xyear = participation_codes.k_student_xyear
        and stg_stu_discipline_incident_behaviors.k_discipline_incident = participation_codes.k_discipline_incident
    join dim_student on stg_stu_discipline_incident_behaviors.k_student = dim_student.k_student
    join dim_school on stg_stu_discipline_incident_behaviors.k_school = dim_school.k_school
    join dim_discipline_incident on stg_stu_discipline_incident_behaviors.k_discipline_incident = dim_discipline_incident.k_discipline_incident
    left join xwalk_discipline_behaviors
        on stg_stu_discipline_incident_behaviors.behavior_type = xwalk_discipline_behaviors.behavior_type
        
    -- custom data sources
    {{ add_cds_joins_v2(cds_model_config=custom_data_sources_name) }}
)
select *
from formatted