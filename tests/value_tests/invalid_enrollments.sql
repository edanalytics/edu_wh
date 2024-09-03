/*
**What is this test?**
This test finds records where student-school association data have 
"invalid" values, because the student:
a) exited before the first day of school (sometimes "invalid" for analytics)
b) exited before entry date (should be impossible)
c) have a withdraw code that denotes an invalid enrollment record

**When is this important to resolve?**
Sometimes these records will simply be reality, e.g. students who withdraw
before the school year starts. However, if these reflect data errors from the
source system (likely the case for b) above), they should be resolved.

**How to resolve?**
Determine from the source system whether these records are accurate, and if not,
debug the source system or contact its administrator.
*/

{{
  config(
      store_failures = true,
      severity       = 'warn'
    )
}}
with stg_stu_school as (
    select * from {{ ref('stg_ef3__student_school_associations') }}
),
first_school_day as (
    select k_school_calendar, min(calendar_date) as calendar_date
    from {{ ref('dim_calendar_date') }}
    group by 1
),
q as (
    select
        exit_withdraw_date < first_school_day.calendar_date as exit_before_first_day,
        exit_withdraw_date < entry_date as exit_before_entry,
        {% set excl_withdraw_codes =  var('edu:enroll:exclude_withdraw_codes')  %}
        {% if excl_withdraw_codes | length -%}
          {% if excl_withdraw_codes is string -%}
            {% set excl_withdraw_codes = [excl_withdraw_codes] %}
          {%- endif -%}
          stg_stu_school.exit_withdraw_type in (
          '{{ excl_withdraw_codes | join("', '") }}'
          ) as excluded_withdraw_code,
        {% else %}
            null as excluded_withdraw_code,
        {% endif %}
        stg_stu_school.*
    from stg_stu_school
    left join first_school_day
        on stg_stu_school.k_school_calendar = first_school_day.k_school_calendar
)
select *
from q
where exit_before_first_day
    or exit_before_entry
    or excluded_withdraw_code