/*
Find cases where a custom demographic source is not unique on k_student. This will break dim_student when the source is joined.
*/
{{
  config(
      store_failures = true,
      severity       = 'error'
    )
}}

{% set custom_data_sources = var("edu:stu_demos:custom_data_sources") %}
{% if custom_data_sources is not none and custom_data_sources | length -%}
  -- if custom data sources are configured, loop over them and count duplicates
  with
  {% for source in custom_data_sources -%}
   {{source}}_dupes as (
    select
      '{{source}}' as data_source,
      k_student,
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
  select top 0 *
  from (select
          null as data_source,
          null as k_student,
          null as n_records
       ) blank_subquery
{%- endif %}
