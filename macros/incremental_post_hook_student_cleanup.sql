{% macro incremental_post_hook_student_cleanup() %}
{% if is_incremental() %}
    DELETE FROM {{ this }} WHERE k_student_xyear not in 
        (select k_student_xyear 
         from {{ ref('dim_student') }}
        );
    UPDATE {{ this }}
    SET k_student = NULL
    WHERE k_student not in 
        (select k_student 
         from {{ ref('dim_student') }}
        );
{% endif %}
{% endmacro%}