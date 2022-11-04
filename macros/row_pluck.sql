{% macro row_pluck(obj, key, column, preferred, where=None) %}
{#- 
    Pluck a single row out of a set, where we have a preference but want 
    to fall back if that preference isn't present.

    For instance: in a list of addresses, we may prefer 'Home', but otherwise
    want to fall back to whatever is populated.

    In this implementation, the fallback case will be the first alphabetical.

    Args:
    obj: an object name. Can be a call to `ref` or the name of a preceding CTE
    key: The grain at which we want to return a single row
    column: The column over which we have a preference, e.g. the unique key of the sub-list.
    preferred: The value of column that we'd prefer to keep, if available.
    where: (Optional) a filtering expression to remove rows from consideration.
 -#}
  {% if key is string %}
    {% set key_list= [key] %}  
    {% else %}
    {% set key_list = key %}
  {% endif %}
      select 
        *,
        {{ column }} = '{{ preferred }}' as is_preferred 
      from {{ obj }}
      {% if where -%}
       where {{ where }} 
      {%- endif %}
      qualify 1 = row_number() over( 
        partition by {{ key_list | join(', ') }} 
        order by is_preferred desc nulls last, {{ column }} )
{% endmacro %}