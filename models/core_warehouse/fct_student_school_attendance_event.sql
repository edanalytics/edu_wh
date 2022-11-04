{{
  config(
    post_hook=[
        "alter table {{ this }} add primary key (k_student, k_school, k_calendar_date)",
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
    `attendance_event_date` into `k_calendar_date`.
    But these grains aren't guaranteed to align-- a student could have multiple
    enrollments at the same school in the same year-- so we need to make sure
    we don't multiply the attendance rows by the number of enrollments.
    The most accurate way to do this would be to specify the join such that 
    the attendance_event_date is within the range of the enrollment's entry_date
    and exit_withdraw_date, because this would account for the case where a 
    student's multiple enrollments each made use of different calendars.
    (This would introduce a second problem though: it would not guarantee a
    single row return if a student had overlapping enrollments at the same 
    school, which the Ed-Fi model does not prevent.)
    Different calendars per enrollment at the same school in the same year
    seems like a sufficiently unlikely edge-case that we use the singular
    calendar association from the latest enrollment instead, which allows
    for a simpler join, but we could add a test asserting that a student
    with multiple enrollments at the same school in the same year all also use
    the same calendar to be sure.
    */
    select * from {{ ref('fct_student_school_association') }}
    where is_latest_annual_entry
),
xwalk_att_events as (
    select * from {{ ref('xwalk_attendance_events') }}
),
formatted as (
    select 
        dim_student.k_student,
        dim_student.k_student_xyear,
        dim_school.k_school,
        dim_calendar_date.k_calendar_date,
        dim_session.k_session,
        stg_stu_sch_attend.tenant_code,
        stg_stu_sch_attend.attendance_event_category,
        stg_stu_sch_attend.attendance_event_reason,
        xwalk_att_events.is_absent,
        stg_stu_sch_attend.event_duration,
        stg_stu_sch_attend.arrival_time,
        stg_stu_sch_attend.departure_time,
        stg_stu_sch_attend.educational_environment
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
    join xwalk_att_events
        on stg_stu_sch_attend.attendance_event_category = xwalk_att_events.attendance_event_descriptor
)
select * from formatted