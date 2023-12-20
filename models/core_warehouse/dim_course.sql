{{
  config(
    post_hook=[
        "alter table {{ this }} add primary key (k_course)",
    ]
  )
}}

{# Load custom data sources from var #}
{% set custom_data_sources = var("edu:course:custom_data_sources") %}

with stg_course as (
    select * from {{ ref('stg_ef3__courses') }}
),
bld_ef3__wide_ids_course as (
    select * from {{ ref('bld_ef3__wide_ids_course') }}
),
formatted as (
    select 
        stg_course.k_course,
        stg_course.tenant_code,
        stg_course.school_year,
        stg_course.course_code,
        stg_course.course_title,
        {{ accordion_columns(
            source_table='bld_ef3__wide_ids_course', 
            exclude_columns=['tenant_code', 'api_year', 'k_course']) }}
        stg_course.course_description,
        stg_course.ed_org_id,
        stg_course.ed_org_type,
        stg_course.academic_subject,
        stg_course.career_pathway,
        stg_course.course_defined_by,
        stg_course.gpa_applicability,
        stg_course.date_course_adopted,
        stg_course.is_high_school_course_requirement,
        stg_course.max_completions_for_credit,
        stg_course.maximum_available_credits,
        stg_course.maximum_available_credit_type,
        stg_course.maximum_available_credit_conversion,
        stg_course.minimum_available_credits,
        stg_course.minimum_available_credit_type,
        stg_course.minimum_available_credit_conversion,
        stg_course.number_of_parts,
        stg_course.time_required_for_completion

        -- custom indicators
        {% if custom_data_sources is not none and custom_data_sources | length -%}
          {%- for source in custom_data_sources -%}
            {%- for indicator in custom_data_sources[source] -%}
              , {{ custom_data_sources[source][indicator]['where'] }} as {{ indicator }}
            {%- endfor -%}
          {%- endfor -%}
        {%- endif %}
    from stg_course
    left join bld_ef3__wide_ids_course 
        on stg_course.k_course = bld_ef3__wide_ids_course.k_course
    
    -- custom data sources
    {% if custom_data_sources is not none and custom_data_sources | length -%}
      {%- for source in custom_data_sources -%}
        left join {{ ref(source) }}
          on stg_course.k_course = {{ source }}.k_course
      {% endfor %}
    {%- endif %}
)
select * from formatted
order by tenant_code, school_year desc, k_course
