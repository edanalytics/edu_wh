{% macro bld_star(from, except=None) %} 
    {% set except_list = ['tenant_code', 'api_year'] %}
    {% if except %}
      {% do except_list.append(except) %} 
    {% endif %}
  {{ dbt_utils.star(from, except=except_list) }}
{% endmacro %}