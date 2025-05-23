{% macro incremental_pre_hook_by_year() %}
{% if is_incremental() %}
    DELETE FROM {{ this }} WHERE school_year in (select max(school_year) from {{ this }})
{% endif %}
{% endmacro %}