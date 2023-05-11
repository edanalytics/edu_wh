{{
  config(
    post_hook=[
        "alter table {{ this }} add primary key (k_student, k_course_section, attendance_event_date)",
        "alter table {{ this }} add constraint fk_{{ this.name }}_student foreign key (k_student) references {{ ref('dim_student') }}",
        "alter table {{ this }} add constraint fk_{{ this.name }}_course_section foreign key (k_course_section) references {{ ref('dim_course_section') }}",
    ]
  )
}}

with stg_stu_section_attendance as (
    select * from {{ ref('stg_ef3__student_section_attendance_events') }}
),
dim_student as (
    select * from {{ ref('dim_student') }}
),
dim_course_section as (
    select * from {{ ref('dim_course_section') }}
),
xwalk_att_events as (
    select * from {{ ref('xwalk_attendance_events') }}
),
formatted as (
    select 
        dim_student.k_student,
        dim_student.k_student_xyear,
        dim_course_section.k_school,
        dim_course_section.k_course_section,
        stg_stu_section_attendance.tenant_code,
        stg_stu_section_attendance.attendance_event_date,
        stg_stu_section_attendance.attendance_event_category,
        stg_stu_section_attendance.attendance_event_reason,
        xwalk_att_events.is_absent,
        stg_stu_section_attendance.event_duration,
        stg_stu_section_attendance.section_attendance_duration,
        stg_stu_section_attendance.arrival_time,
        stg_stu_section_attendance.departure_time,
        stg_stu_section_attendance.educational_environment
    from stg_stu_section_attendance
    join dim_student
        on stg_stu_section_attendance.k_student = dim_student.k_student
    join dim_course_section
        on stg_stu_section_attendance.k_course_section = dim_course_section.k_course_section
    join xwalk_att_events
        on stg_stu_section_attendance.attendance_event_category = xwalk_att_events.attendance_event_descriptor
)
select * from formatted