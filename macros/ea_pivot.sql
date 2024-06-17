{#
Pivot values from rows to columns.
This extends dbt_utils.pivot with a few changes:
1. Type casting. Most frequently used to coerce aggregated numeric indicators to boolean
2. Default quote_identifiers to false 


Example:

    Input: `public.test`

    | size | color |
    |------+-------|
    | S    | red   |
    | S    | blue  |
    | S    | red   |
    | M    | red   |

    select
      size,
      {{ dbt_utils.pivot('color', dbt_utils.get_column_values('public.test',
                                                              'color')) }}
    from public.test
    group by size

    Output:

    | size | red | blue |
    |------+-----+------|
    | S    | 2   | 1    |
    | M    | 1   | 0    |

Arguments:
    column: Column name, required
    values: List of row values to turn into columns, required
    alias: Whether to create column aliases, default is True
    agg: SQL aggregation function, default is sum
    cmp: SQL value comparison, default is =
    cast: If filled, will cast the wide columns to a different sql data type.
    prefix: Column alias prefix, default is blank
    suffix: Column alias postfix, default is blank
    then_value: Value to use if comparison succeeds, default is 1
    else_value: Value to use if comparison fails, default is 0
    quote_identifiers: Whether to surround column aliases with double quotes, default is false
    distinct: Whether to use distinct in the aggregation, default is False
#}


{% macro ea_pivot(column,
                values,
                alias=True,
                agg='sum',
                cmp='=',
                cast='',
                prefix='',
                suffix='',
                then_value=1,
                else_value=0,
                quote_identifiers=False,
                distinct=False) %}
  {% for value in values %}
    {{ agg }}(
      {%- if distinct %} distinct {% endif -%}
      case
        when {{ column }} {{ cmp }} '{{ dbt.escape_single_quotes(value) }}'
            then {{ then_value }}
        else {{ else_value }}
      end
    ){% if cast %}::{{cast}} {% endif %}
    {% if alias -%}
      {% if quote_identifiers -%}
            as {{ adapter.quote(prefix ~ value ~ suffix) }}
      {% else -%}
        as {{ dbt_utils.slugify(prefix ~ value ~ suffix) }}
      {% endif -%}
    {%- endif -%}
    {%- if not loop.last %},{% endif %}
  {% endfor %}
{% endmacro %}
