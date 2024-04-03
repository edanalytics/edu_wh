/*
**What is this test?**
This test finds records of overlapping dates in xwalk used to derive school year
from assessment administration dates.

**When is this important to resolve?**
When any overlapping dates are found.

**How to resolve?**
Adjust dates to make mutually exclusive.
*/

{{
  config(
      store_failures = true,
      severity       = 'error'
    )
}}
with xwalk_assess_dates as (
    select * from  {{ ref('xwalk_assessment_school_year_dates') }}
),
overlapping_dates as (
    select
      dates1.assessment_identifier,
      dates1.namespace,
      dates1.school_year as school_year_1,
      dates2.school_year as school_year_2,
      dates1.start_date as start_date_1,
      dates1.end_date as end_date_1,
      dates2.start_date as start_date_2,
      dates2.end_date as end_date_2
    from xwalk_assess_dates as dates1
    -- self join to find overlap within the xwalk
    join xwalk_assess_dates as dates2
      on equal_null(dates1.assessment_identifier, dates2.assessment_identifier)
      and equal_null(dates1.namespace, dates2.namespace)
      and dates1.school_year != dates2.school_year
      -- join where overlapping, inclusive, bc code uses 'between' which is inclusive on thresholds
      and (
        dates1.start_date <= dates2.end_date
        and
        dates1.end_date >= dates2.start_date
      )
)

select * from overlapping_dates