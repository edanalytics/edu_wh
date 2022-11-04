{% macro msr_chronic_absentee(attendance_rate, days_enrolled) %}
  {{ attendance_rate }} < {{ var('edu:attendance:chronic_absence_threshold') }}
    and {{ days_enrolled }} >= {{ var('edu:attendance:chronic_absence_min_days') }}
{% endmacro %}