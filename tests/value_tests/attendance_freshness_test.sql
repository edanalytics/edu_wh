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
select *
from {{ ref('attendance_freshness') }}
where (current_date() - max_date) > 7
-- try to avoid warnings when school is out
and monthname(current_date()) not in ('Jul', 'Aug')