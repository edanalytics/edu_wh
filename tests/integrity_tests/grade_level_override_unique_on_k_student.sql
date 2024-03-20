/*
**What is this test?**
This test finds records where a grade_level_override for dim_student is NOT unique on k_student. If true,
the grain of dim_student could be blown up in a dangerous way, because of the join in bld_ef3__stu_grade_level

**When is this important to resolve?**
Immediately, if any rows are returned.

**How to resolve?**
Update the model used as a grade_level_override source to ensure it is unique on k_student (or make a build model for that purpose).

*/
{{
  config(
      store_failures = true,
      severity       = 'error'
    )
}}

{% if var('edu:stu_demos:grade_level_override', False) -%}
  {% set source = var('edu:stu_demos:grade_level_override')['source'] %}
  select
    '{{source}}' as data_source,
    k_student,
    count(*) as n_records
  from {{ref(source)}}
  group by 1,2
  having count(*) > 1
{%- else %}
  -- if no custom grade_level_override configured, force test to return a zero row table
  select top 0 *
  from (select
          null as data_source,
          null as k_student,
          null as n_records
       ) blank_subquery
{%- endif %}
