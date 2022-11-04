{#
Pivot values from rows to columns using a crosswalk to transform the labels.

Example:

    Given data to be pivoted:

    | stu  | characteristic          |
    |------+-------------------------|
    | 1    | Economic Disadvantaged  |
    | 1    | Gifted                  |
    | 2    | Economic Disadvantaged  |
    | 3    | Homeless                |
    | 4    | NULL                    |

    And a crosswalk:

    | old_label               | new_label      |
    +-------------------------+----------------|
    | Economic Disadvantaged  | is_econ_disadv |
    | Gifted                  | is_gifted      |
    | Homeless                | is_homeless    |


    select
      stu,
      {{ dbt_utils.pivot(column='characteristic',
                         cmp_col_name='old_label',
                         alias_col_name='new_label',
                         xwalk_ref='char_xwalk',
                         null_false=True,
                         cast='boolean') }}
    from public.test
    group by stu

    Output:

    | stu  | is_econ_disadv | is_gifted | is_homeless |
    |------+----------------+-----------+-------------|
    | 1    | True           | True      | False       |
    | 2    | True           | False     | False       |
    | 3    | False          | False     | True        |
    | 4    | False          | False     | False       |

Arguments:
    column: Column name, required
    cmp_col_name: The name of the xwalk column that will be compared to `column`
    alias_col_name: The name of the xwalk column that will be used for the alias.
    agg: SQL aggregation function, default is sum
    cmp: SQL value comparison, default is =
    null_false: Should NULL be considered FALSE?
    cast: Coerce the output data type? (e.g. take a sum to a boolean)
    prefix: Column alias prefix, default is blank
    suffix: Column alias postfix, default is blank
    then_value: Value to use if comparison succeeds, default is 1
    else_value: Value to use if comparison fails, default is 0
    quote_identifiers: Whether to surround column aliases with double quotes, default is true
    distinct: Whether to use distinct in the aggregation, default is False
#}

-- alternative pivot with the option to cast the resulting value
-- and treat nulls as false
{% macro alias_pivot(column,
               cmp_col_name,
               alias_col_name,
               xwalk_ref,
               agg='sum',
               cmp='=',
               null_false=False,
               cast='',
               prefix='',
               suffix='',
               then_value=1,
               else_value=0,
               quote_identifiers=False,
               distinct=False) %}
  {%- set sql_statement  -%}
    select 
      {{cmp_col_name}} as "comp_value",
      {{alias_col_name}} as "alias_value"
    from {{ ref(xwalk_ref) }}
  {%- endset -%}
  {%- set val_dict = dbt_utils.get_query_results_as_dict(sql_statement) -%}
  {% for value in val_dict['comp_value'] %}
    {% set alias = val_dict['alias_value'][loop.index0] %}
  
    {{ agg }}(
      {% if distinct %} distinct {% endif -%}
      case
      {% if null_false %}
      when equal_null({{column}}, '{{ dbt_utils.escape_single_quotes(value) }}')
      {% else %}
      when {{ column }} {{ cmp }} '{{ dbt_utils.escape_single_quotes(value) }}'
      {% endif %}
        then {{ then_value }}
      else {{ else_value }}
      end
    ){% if cast %}::{{ cast }} {% endif %} 
    {% if quote_identifiers %}
          as {{ adapter.quote(prefix ~ alias ~ suffix) }}
    {% else %}
      as {{ dbt_utils.slugify(prefix ~ alias ~ suffix) }}
    {% endif -%}
    {% if not loop.last %},{% endif %}
  {% endfor %}
{% endmacro %}