{# 
This macro takes derives school years from a date column.
#}

{% macro derive_school_year(date_column) %}

    {# {% set date_lower_bound = 
            "concat_ws('/', '{{var('edu:school_year:start_month')}}', 
                           '{{var('edu:school_year:start_day')}}', 
                           year(date_column::date))::date" %}

    {% set date_upper_bound = 
            "concat_ws('/', '{{var('edu:school_year:start_month')}}', 
                           '{{var('edu:school_year:start_day')}}', 
                           (year(date_column::date)::int + 1))::date" %} #}

    case
        when {{date_column}}::date >= 
            concat_ws('/', '{{var("edu:school_year:start_month")}}', 
                           '{{var("edu:school_year:start_day")}}', 
                           year({{date_column}}::date))::date
            and {{date_column}}::date < 
            concat_ws('/', '{{var("edu:school_year:start_month")}}', 
                           '{{var("edu:school_year:start_day")}}', 
                           (year({{date_column}}::date)::int + 1))::date
            then (year({{date_column}}::date)::int + 1)
        else year({{date_column}}::date)
    end

{% endmacro %}
