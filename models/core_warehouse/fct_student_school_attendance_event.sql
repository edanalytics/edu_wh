{{
  config(
    post_hook=[
        "alter table {{ this }} add primary key (k_student, k_school, k_session, attendance_event_category, k_calendar_date)",
        "alter table {{ this }} add constraint fk_{{ this.name }}_student foreign key (k_student) references {{ ref('dim_student') }}",
        "alter table {{ this }} add constraint fk_{{ this.name }}_school foreign key (k_school) references {{ ref('dim_school') }}",
    ]
  )
}}

with stg_stu_sch_attend as (
    select * from {{ ref('stg_ef3__student_school_attendance_events') }}
),
dim_student as (
    select * from {{ ref('dim_student') }}
),
dim_school as (
    select * from {{ ref('dim_school') }}
),
dim_session as (
    select * from {{ ref('dim_session') }}
),
dim_calendar_date as (
    select * from {{ ref('dim_calendar_date') }}
),
fct_student_school_assoc as (
    /*
    We bring in this table to get the `k_school_calendar` we need to turn
    `attendance_event_date` into `k_calendar_date`. Because a student could have
    multiple enrollments at the same school in the same year, we specify the join
    such that the attendance_event_date is within the range of the enrolmment's
    entry_date and exit_withdraw_date. This accounts for the case where a student's
    multiple enrollments each made use of different calendars. However, if a 
    student has overlapping enrollments at the same school, multiple rows will
    be returned for each date. Therefore we must introduce a deduplication step.
    */
    select 
        *,
        date(coalesce(exit_withdraw_date,getdate())) - entry_date as enrollment_length
    from {{ ref('fct_student_school_association') }}
),
xwalk_att_events as (
    select * from {{ ref('xwalk_attendance_events') }}
),
joined as (
    select 
        dim_student.k_student,
        dim_student.k_student_xyear,
        dim_school.k_school,
        dim_calendar_date.k_calendar_date,
        dim_session.k_session,
        stg_stu_sch_attend.tenant_code,
        stg_stu_sch_attend.school_year,
        stg_stu_sch_attend.pull_timestamp,
        stg_stu_sch_attend.last_modified_timestamp,
        stg_stu_sch_attend.attendance_event_category,
        stg_stu_sch_attend.attendance_event_reason,
        xwalk_att_events.is_absent,
        stg_stu_sch_attend.event_duration,
        stg_stu_sch_attend.school_attendance_duration,
        stg_stu_sch_attend.arrival_time,
        stg_stu_sch_attend.departure_time,
        stg_stu_sch_attend.educational_environment,
        fct_student_school_assoc.enrollment_length,
        fct_student_school_assoc.entry_date,
        fct_student_school_assoc.exit_withdraw_date
    from stg_stu_sch_attend
    join dim_student
        on stg_stu_sch_attend.k_student = dim_student.k_student
    join dim_school
        on stg_stu_sch_attend.k_school = dim_school.k_school
    join dim_session
        on stg_stu_sch_attend.k_session = dim_session.k_session
    join fct_student_school_assoc
        on stg_stu_sch_attend.k_student = fct_student_school_assoc.k_student
        and stg_stu_sch_attend.k_school = fct_student_school_assoc.k_school
    join dim_calendar_date
         on fct_student_school_assoc.k_school_calendar = dim_calendar_date.k_school_calendar
         and stg_stu_sch_attend.attendance_event_date = dim_calendar_date.calendar_date
         and dim_calendar_date.calendar_date between fct_student_school_assoc.entry_date and coalesce(fct_student_school_assoc.exit_withdraw_date,current_date())
    join xwalk_att_events
        on stg_stu_sch_attend.attendance_event_category = xwalk_att_events.attendance_event_descriptor
),
deduped as (
    -- account for multiple overlapping enrollments
    {{
        dbt_utils.deduplicate(
            relation='joined',
            partition_by='k_student, k_school,k_calendar_date',
            order_by='enrollment_length desc, entry_date desc, exit_withdraw_date desc'
        )
    }}
),
formatted as (
    select
        k_student,
        k_student_xyear,
        k_school,
        k_calendar_date,
        k_session,
        tenant_code,
        attendance_event_category,
        attendance_event_reason,
        is_absent,
        event_duration,
        school_attendance_duration,
        arrival_time,
        departure_time,

        educational_environment,
        last_modified_timestamp
        {# add any extension columns configured from stg_ef3__student_school_attendance_events #}
        {{ edu_edfi_source.extract_extension(model_name='stg_ef3__student_school_attendance_events', flatten=False) }}
    from deduped
)
select * from formatted
