{{
  config(
    post_hook=[
        "alter table {{ this }} add primary key (k_student, k_discipline_incident, behavior_type)",
        "alter table {{ this }} add constraint fk_{{ this.name }}_student foreign key (k_student) references {{ ref('dim_student') }}",
        "alter table {{ this }} add constraint fk_{{ this.name }}_school foreign key (k_school) references {{ ref('dim_school') }}"
    ]
  )
}}

with stg_stu_discipline_incident_behaviors as (
    select * from {{ ref('stg_ef3__student_discipline_incident_behavior_associations') }}
),
dim_student as (
    select * from {{ ref('dim_student') }}
),
dim_school as (
    select * from {{ ref('dim_school') }}
),
dim_discipline_incidents as (
    select * from {{ ref('dim_discipline_incidents') }}
),
xwalk_discipline_behaviors as (
    select * from {{ ref('xwalk_discipline_behaviors') }}
),
participation_codes as (
    select
        k_student,
        k_discipline_incident,
        array_agg(participation_code) as participation_codes_array
    from {{ ref('stg_ef3__student_discipline_incident_behavior_associations__participation_codes') }}
    group by k_student, k_discipline_incident
),
formatted as (
    select 
        dim_student.k_student,
        dim_school.k_school,
        dim_discipline_incidents.k_discipline_incident,
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
            when 1 = row_number() over (partition by dim_student.k_student, dim_school.k_school, stg_stu_discipline_incident_behaviors.k_discipline_incident order by xwalk_discipline_behaviors.severity_order desc)
                then true
            else false
        end as is_most_severe,
        -- there is typically only a single value here, choosing the first option for analytical use cases
        participation_codes.participation_codes_array[0] as participation_code,
        participation_codes.participation_codes_array
    from stg_stu_discipline_incident_behaviors
    left join participation_codes 
        on stg_stu_discipline_incident_behaviors.k_student = participation_codes.k_student
        and stg_stu_discipline_incident_behaviors.k_discipline_incident = participation_codes.k_discipline_incident
    join dim_student on stg_stu_discipline_incident_behaviors.k_student = dim_student.k_student
    join dim_school on stg_stu_discipline_incident_behaviors.k_school = dim_school.k_school
    join dim_discipline_incidents on stg_stu_discipline_incident_behaviors.k_discipline_incident = dim_discipline_incidents.k_discipline_incident
    left join xwalk_discipline_behaviors
        on stg_stu_discipline_incident_behaviors.behavior_type = xwalk_discipline_behaviors.behavior_type
)
select *
from formatted