{# 
Check if the value of a field is NOT in a given list of items.

Arguments: 
  field: name of the column that you want to search in
  excluded_items: comma separated list of items that you want to check for exclusion
    example: ['aaa', 'bbb']

Returns:
  boolean:
    - true if the value in the field is NOT in the list of excluded_items
    - true if excluded_items is null
    - false if the value of the field IS in the list of excluded_items
#}

{% macro value_not_in_list(field, excluded_items) %}

  {% if excluded_items is not none and excluded_items | length -%}
    {% if excluded_items is string -%}
      {% set excluded_items = [excluded_items] %}
    {%- endif -%}
    {{ field }} not in (
    {%- for val in excluded_items -%}
      '{{ val }}'
      {%- if not loop.last %},{% endif -%}
    {%- endfor -%}
    )
  {%- else -%}
  true
  {%- endif -%}

{% endmacro %}