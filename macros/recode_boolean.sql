{#
This macro reclassifies text fields to boolean, and includes commonly observed T/F indicators as defaults.

Arguments:
    col: The column name to be recoded. Note that it will be automatically `lower`ed, so you need not worry about case.
    true_cases: A list of strings that will be coerced to True
    false_cases: A list of strings that will be coerced to False
    default_case: What value should non-matching strings take? This should typically be either 'null' or 'false',

Usage: 
    Called directly on a column within a select statement.
#}

{% macro recode_boolean(col, 
                        true_cases=['yes', 'y', 'true', 't', '1', '1.0'], 
                        false_cases=['no', 'n', 'false', 'f', '0', '0.0'], 
                        default_case='null') %}
    case
    {% if true_cases is not none -%}
        when lower({{ col }}::text) in ('{{ true_cases | join("','") }}')
        then TRUE
    {% endif -%}
    {% if false_cases is not none -%}
        when lower({{ col }}::text) in ('{{ false_cases | join("','") }}')
        then FALSE
    {% endif -%}
    else {{ default_case }}
    end
{% endmacro %}