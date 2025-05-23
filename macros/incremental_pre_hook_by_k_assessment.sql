{% macro incremental_pre_hook_by_k_assessment() %}
{% if is_incremental() %}
    DELETE FROM {{ this }} WHERE k_assessment in 
        (select k_assessment 
         from {{ ref('stg_ef3__student_assessments') }}
         where last_modified_timestamp
        )
{% endif %}
{% endmacro %}
