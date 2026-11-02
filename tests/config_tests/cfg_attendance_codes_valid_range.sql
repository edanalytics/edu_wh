-- SPDX-FileCopyrightText: 2022 Education Analytics
-- SPDX-License-Identifier: MPL-2.0

{{
  config(
      store_failures = true,
      severity       = 'error'
    )
}}
select distinct is_absent
from {{ ref('xwalk_attendance_events') }}
where is_absent not between 0 and 1
