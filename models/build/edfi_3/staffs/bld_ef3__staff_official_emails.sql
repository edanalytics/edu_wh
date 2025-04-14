{{
  config(
    materialized = 'view',
    )
}}

{% set work_email_codes = var('edu:staff:official_email_codes', ['Work']) %}
{% if work_email_codes is string %} {% set work_email_codes = [work_email_codes] %} {% endif %}

select *
from {{ ref('bld_ef3__staff_emails') }}
where email_type in {{ work_email_codes | join(', ') }} 
