with stg_stu_discipline_incident_non_offenders as (
    select * from {{ ref('stg_ef3__student_discipline_incident_non_offender_associations') }}
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
participation_codes as (
    select
        k_student,
        k_discipline_incident,
        array_agg(participation_code) as participation_codes_array
    from {{ ref('stg_ef3__student_discipline_incident_non_offender_associations__participation_codes') }}
    group by k_student, k_discipline_incident
),
formatted as (
    select 
        dim_student.k_student,
        dim_school.k_school,
        dim_discipline_incidents.k_discipline_incident,
        stg_stu_discipline_incident_non_offenders.tenant_code,
        stg_stu_discipline_incident_non_offenders.incident_id,
        false as is_offender,
        -- todo: name of this col?
        -- there is typically only a single value here, choosing the first option for analytical use cases
        {{ extract_descriptor('v_discipline_incident_participation_codes[0]:disciplineIncidentParticipationCodeDescriptor::string') }} as participation_code,
        participation_codes.participation_codes_array
    from stg_stu_discipline_incident_non_offenders
    join participation_codes 
        on stg_stu_discipline_incident_non_offenders.k_student = participation_codes.k_student
        and stg_stu_discipline_incident_non_offenders.k_discipline_incident = participation_codes.k_discipline_incident
    join dim_student on stg_stu_discipline_incident_non_offenders.k_student = dim_student.k_student
    join dim_school on stg_stu_discipline_incident_non_offenders.k_school = dim_school.k_school
    join dim_discipline_incidents on stg_stu_discipline_incident_non_offenders.k_discipline_incident = dim_discipline_incidents.k_discipline_incident
)
select *
from formatted