{% macro date_within_end_date(date_col, end_date, inclusive=true) %}
{#- 
    Produces sql to determine if a date is within an end date or not.

    The inclusivity of the end_date is what matters here. If end_date
    is inclusive and date_col is less than or equal to the end_date, 
    then it is within the end_date. But if the end date is exclusive and
    date_col equals end_date then it is not within the end date.

    Inclusivity is assumed.

    Edfi makes no mention of whether end_dates should be considered 
    inclusive or exclusive. When asked they will say to do what makes
    sense of your own implementation of Edfi.

    If end_date is null, then date_col is within the end date.

    Args:
    date_col: the date to be compared against the end_date
    end_date: the end date of some date range.
    inclusive: should the end_date be considered inclusive or not
 -#}
    {% if inclusive %}
        ({{ end_date }} is null or {{ date_col }} <= {{ end_date }})
    {% else %}
        ({{ end_date }} is null or {{ date_col }} < {{ end_date }})
    {% endif %}
{% endmacro %}

{% macro day_count_in_range(begin_date, end_date, inclusive=true) %}
{#- 
    Inclusivity for an end date (or begin date, for that matter) matters
    when trying to determine the dates within a date range. If the end date
    is exclusive, then that day does not count towards the day count.

    If the end date is null, then we will be calculating the day count as of 
    today AND today COUNTS as a day in the range, regardless of inclusivity.

    Args:
    begin_date: the start of the date range
    end_date: the end of the date range
    inclusive: should the end_date be considered inclusive or not
 -#}
    {{ return(adapter.dispatch('day_count_in_range', 'edu_wh')(begin_date, end_date, inclusive=true)) }}
{% endmacro %}

{% macro snowflake__day_count_in_range(begin_date, end_date, inclusive) -%}
    {% if inclusive %}
        date(coalesce({{ end_date }}, getdate())) - {{ begin_date }} + 1
    {% else %}
        date(coalesce({{ end_date }}, getdate())) - {{ begin_date }}
    {% endif %}
{%- endmacro %}

{% macro databricks__day_count_in_range(begin_date, end_date, inclusive) -%}
    {% if inclusive %}
        date_diff(dateadd(day, 1, coalesce({{ end_date }}, CURRENT_DATE())), {{ begin_date }})
    {% else %}
        date_diff(coalesce({{ end_date }}, dateadd(day, 1, CURRENT_DATE())), {{ begin_date }})
    {% endif %}
{%- endmacro %}