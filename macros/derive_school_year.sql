{# 
This macro takes derives school years from a date column.

The date column will be compared to the default school year start day and month.
#}

{% macro derive_school_year(date_column) %}
    {% if var("edu:school_year:start_month", None) and var("edu:school_year:start_day", None) %}
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
    {% else %}
    null
    {% endif %}
{% endmacro %}
