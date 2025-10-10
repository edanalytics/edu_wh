/*
**What is this test?**
This test finds records where attendance data for a tenant is stale (> 7 days old).
This could be due to issues syncing from source system to ODS or from ODS
to warehouse.

**When is this important to resolve?**
When up-to-date daily attendance is reported.

**How to resolve?**
Check whether there are errors pulling attendance records from ODS, 
or if there are errors pushing data from source system to ODS.

*/
{{
  config(
      store_failures = true,
      severity       = 'warn'
    )
}}
with filtered as (
  select *,
    datediff(DAY, max_date, current_date()) as days_since_last_attendance_event
  from {{ ref('attendance_freshness') }}
  where datediff(DAY, max_date, current_date()) > 7
  -- try to avoid warnings when school is out
    and month(current_date()) not in (7, 8)
)
select count(*) as failed_row_count, tenant_code, school_year from filtered
group by all
having count(*) > 1
