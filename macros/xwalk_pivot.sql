{% macro xwalk_pivot(column, 
                value_xwalk, 
                alias=True,
                agg='min',
                cmp='=',
                cast='',
                prefix='',
                suffix='',
                then_value=1,
                else_value=0,
                quote_identifiers=False,
                distinct=False) %}

  {%- for key, value in value_xwalk.items() %}
    {{ agg }}(
      {% if distinct %} distinct {% endif %}
      case
      when {{ column }} {{ cmp }} '{{ key }}'
        then {{ then_value }}
      else {{ else_value }}
      end
    ){% if cast %}::{{ cast }} {% endif %} 
    {% if alias -%}
      {%- if quote_identifiers -%}
            as {{ adapter.quote(prefix ~ value ~ suffix) }}
      {%- else -%}
        as {{ dbt_utils.slugify(prefix ~ value ~ suffix) }}
      {%- endif -%}
    {%- endif -%}
    {%- if not loop.last %},{% endif %}
  {% endfor %}
{% endmacro %}