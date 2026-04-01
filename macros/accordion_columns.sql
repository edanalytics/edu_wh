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
  - add_trailing_comma: if this value is specified, a trailing comma will be added to the last
    column. If not passed in, a trailing comma is added for backward compatibility
-#}

{% macro accordion_columns(source_table, exclude_columns, source_alias=none, coalesce_value=none, add_trailing_comma=true) %}
  {%- if source_alias is none -%}
   {%- set source_alias = source_table -%} 
  {%- endif -%}
  {% set keep_cols = dbt_utils.get_filtered_columns_in_relation(
    ref(source_table),
      except=exclude_columns
    ) %}
  {%- if coalesce_value is none %}
    {%- for col in keep_cols %}
      {{ source_alias }}.{{ col }}{% if not loop.last %},{% elif add_trailing_comma %},{% endif %}
    {%- endfor %}
  {%- else -%}
    {%- for col in keep_cols %}
      coalesce({{ source_alias }}.{{ col }}, {{ coalesce_value }}) as {{ col }}{% if not loop.last %},{% elif add_trailing_comma %},{% endif %}
    {%- endfor %}  
  {%- endif -%}
{% endmacro %}