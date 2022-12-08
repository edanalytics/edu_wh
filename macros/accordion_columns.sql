{#- 
This macro helps with configurable, extensible sets of columns that can 
be included in a table.
  - source_table: the reference where the wide columns to be included live
  - exclude_columns: the ID columns used for joining, but not to be included 
    in the table
  - source_alias: the alias of the table within the sql expression. If none (default)
    it will reuse the source_table name.
-#}
{% macro accordion_columns(source_table, exclude_columns, source_alias=none) %}
  {%- if source_alias is none -%}
   {%- set source_alias = source_table -%} 
  {%- endif -%}
  {% set keep_cols = dbt_utils.get_filtered_columns_in_relation(
    ref(source_table),
      except=exclude_columns
    ) %}
  {%- for col in keep_cols %}
    {{ source_alias }}.{{ col }},
  {%- endfor %}
{% endmacro %}