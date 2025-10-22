{#
    Emulates `var()` on a list of variables.

    If any variables return True, True is returned.
    If no variables are defined, `default` is returned.

    Example:
    ```
        {{ any_var([
              'src:program:special_ed:enabled',
              'src:program:homeless:enabled',
              'src:program:language_instruction:enabled',
              'src:program:title_i:enabled',
              'src:program:cte:enabled',
              'src:program:migrant_education:enabled',
              'src:program:food_service:enabled'
            ], default=True
        ) }}
    ```
#}
{% macro any_var(var_list, default=True) %}

    {% set var_values = [] %}

    {% for variable in var_list %}
        {% do var_values.append(var(variable, none)) %}
    {% endfor %}

    {% if True in var_values %}
        {{ return(True) }}
    {% elif False in var_values %}
        {{ return(False) }}
    {% else %}
        {{ return(default) }}
    {% endif %}

{% endmacro %}
