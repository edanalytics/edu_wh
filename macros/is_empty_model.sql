{# Check if a model is empty (has zero rows). Return boolean #}
{% macro is_empty_model(model_name) %}

  {%- set sql_statement  -%}
    select count(*) from {{ ref(model_name) }}
  {%- endset -%}

  {%- set nrow = get_single_value(sql_statement) -%}

  {{ return(nrow == 0) }}

{% endmacro %}