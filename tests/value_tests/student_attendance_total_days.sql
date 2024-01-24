/*
**What is this test?**
This test finds students who have greater than 185 days enrolled contributing
to their attendance rate for a single school year. It's atypical for students
to be enrolled longer than 185 days within a single school  year.

**When is this important to resolve?**
If you have confirmed that this test has uncovered a data error, it's
important to resolve to avoid incorrect reporting of student (and aggregate)
attendance measures.

**How to resolve?**
First confirm that this test has uncovered a real data error. Look at the 
underlying student-school association data for overlapping enrollments 
(or look at the test `overlapping_enrollments`). Also check for errors 
with school calendars. Is the student enrolled in a primary and secondary
school and receiving attendance records for both schools?

Depending on the diagnosis from the underlying data, resolve the issue
in the source system or ODS.
*/
{{
  config(
      store_failures = true,
      severity       = 'warn'
    )
}}
-- find students with total days enrolled larger than typical
with cumulative_attendance as (
    select * from {{ ref('msr_student_cumulative_attendance') }}
)
select *
from cumulative_attendance
where days_enrolled > 185