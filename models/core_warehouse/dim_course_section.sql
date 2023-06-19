{{
  config(
    post_hook=[
        "alter table {{ this }} add primary key (k_course_section)",
        "alter table {{ this }} add constraint fk_{{ this.name }}_course foreign key (k_course) references {{ ref('dim_course') }}",
        "alter table {{ this }} add constraint fk_{{ this.name }}_school foreign key (k_school) references {{ ref('dim_school') }}",
        "alter table {{ this }} add constraint fk_{{ this.name }}_session foreign key (k_session) references {{ ref('dim_session') }}",
    ]
  )
}}

{% set custom_data_sources = var("edu:course_section:custom_data_sources", none) %}

with offering as (
    select * from {{ ref('stg_ef3__course_offerings') }}
),
section as (
    select * from {{ ref('stg_ef3__sections') }}
),
dim_course as (
    select * from {{ ref('dim_course') }}
),
section_chars as (
    select * from {{ ref('bld_ef3__course_char__combined_wide') }}
),
joined as (
    select 
        section.k_course_section,
        dim_course.k_course,
        offering.k_school,
        offering.k_session,
        section.k_location as k_classroom,
        section.tenant_code,
        section.section_id,
        section.section_name,
        offering.local_course_code,
        offering.local_course_title,
        dim_course.course_code,
        dim_course.course_title,
        offering.school_year,
        offering.session_name,
        dim_course.academic_subject,
        dim_course.career_pathway,
        offering.instructional_time_planned,
        section.is_official_attendance_period,

        -- field from custom data source
        {% if custom_data_sources is not none and custom_data_sources | length -%}
          {%- for source in custom_data_sources -%}
            {%- for indicator in custom_data_sources[source] -%}
              {{ custom_data_sources[source][indicator]['where'] }} as {{ indicator }},
            {%- endfor -%}
          {%- endfor -%}
        {%- endif %}

        section.sequence_of_course,

        -- section characteristics
        {{ accordion_columns(
            source_table='bld_ef3__course_char__combined_wide',
            exclude_columns=['tenant_code', 'api_year', 'k_course', 'k_course_offering', 'k_course_section'],
            source_alias='section_chars',
            coalesce_value = 'FALSE'
        ) }}

        section.educational_environment_type,
        section.instruction_language,
        section.medium_of_instruction,
        section.population_served,
        section.available_credits,
        section.available_credit_type,
        section.available_credit_conversion
        -- todo: add characteristic indicators
    from section
    join offering
        on section.k_course_offering = offering.k_course_offering
    join dim_course 
        on offering.k_course = dim_course.k_course
    left join section_chars 
        on section.k_course_section = section_chars.k_course_section
    
    -- custom data source
    {% if custom_data_sources is not none and custom_data_sources | length -%}
      {%- for source in custom_data_sources -%}
        left join {{ ref(source) }}
          on section.k_course_section = {{ source }}.k_course_section
      {% endfor %}
    {%- endif %}

)
select * from joined
order by tenant_code, k_school, k_course_section
