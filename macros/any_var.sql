{#
    Emulates `var()` on a list of variables.

    If any variables return True, True is returned.
    If no variables are defined, `default` is returned.
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
