/*
**What is this test?**
This test finds records where a custom course data source is NOT unique on k_course. If true,
the grain of dim_course could be blown up in a dangerous way, because of the join in the dim_course model.

**When is this important to resolve?**
Immediately, if any rows are returned.

**How to resolve?**
Update the model used as a custom data source to ensure it is unique on k_course (or make a build model for that purpose).

*/
{{
  config(
      store_failures = true,
      severity       = 'error'
    )
}}

{% set custom_data_sources = var("edu:course:custom_data_sources", None) %}
{% if custom_data_sources is not none and custom_data_sources | length -%}
  -- if custom data sources are configured, loop over them and count duplicates
  with
  {% for source in custom_data_sources -%}
   {{source}}_dupes as (
    select
      '{{source}}' as data_source,
      k_course,
      count(*) as n_records
    from {{ref(source)}}
    group by 1,2
    having count(*) > 1
  )
  {%- if not loop.last %},{% endif %}
  {%- endfor %}

  -- stack across all sources
  {% for source in custom_data_sources -%}
     select * from {{source}}_dupes
     {%- if not loop.last %}
     union all
     {% endif %}
  {%- endfor -%}

{%- else %}
  -- if no custom data sources configured, force test to return a zero row table
  select *
  from (select
          null as data_source,
          null as k_course,
          null as n_records
       ) blank_subquery
  limit 0
{%- endif %}
