{#- 
This macro helps with configurable, extensible sets of columns that can 
be included in a table.
  - source_table: the reference where the wide columns to be included live
  - exclude_columns: the ID columns used for joining, but not to be included 
    in the table
  - source_alias: the alias of the table within the sql expression. If none (default)
    it will reuse the source_table name.
  - coalesce_value: if this value is specified, a coalesce function will be added with
    this value as the second parameter (to be returned if the value in the column is null).
    If none (default), the coalesce function will not be added.
-#}
{% macro accordion_columns(source_table, exclude_columns, source_alias=none, coalesce_value=none) %}
  {%- if source_alias is none -%}
   {%- set source_alias = source_table -%} 
  {%- endif -%}
  {% set keep_cols = dbt_utils.get_filtered_columns_in_relation(
    ref(source_table),
      except=exclude_columns
    ) %}
  {%- if coalesce_value is none %}
    {%- for col in keep_cols %}
      {{ source_alias }}.{{ col }},
    {%- endfor %}
  {%- else -%}
    {%- for col in keep_cols %}
      coalesce({{ source_alias }}.{{ col }}, {{ coalesce_value }}) as {{ col }},
    {%- endfor %}  
  {%- endif -%}
{% endmacro %}
