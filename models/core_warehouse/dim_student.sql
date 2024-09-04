{{
  config(
    post_hook=[
        "alter table {{ this }} add primary key (k_student)",
    ]
  )
}}

{# Load custom data sources from var #}
{% set custom_data_sources = var("edu:stu_demos:custom_data_sources") %}


{# If edu var has been configured to make demos immutable, set join var to `k_student_xyear` bc demos are unique by xyear #}
{# otherwise, use k_student bc demos are unique by student+year #}
{%- if var('edu:stu_demos:make_demos_immutable', False) -%}
    {%- set demos_join_var = 'k_student_xyear' -%}
{%- else -%}
    {%- set demos_join_var = 'k_student' -%}
{%- endif -%}

{# customizable: extra indicators to create in the aggregate query #}
{% set custom_special_ed_program_agg_indicators = var('edu:special_ed:custom_program_agg_indicators', None) %}
{% set custom_homeless_program_agg_indicators = var('edu:homeless:custom_program_agg_indicators', None) %}
{% set custom_language_instruction_program_agg_indicators = var('edu:language_instruction:custom_program_agg_indicators', None) %}
{% set custom_title_i_program_agg_indicators = var('edu:title_i:custom_program_agg_indicators', None) %}


with stg_student as (
    select * from {{ ref('stg_ef3__students') }}
),
stu_demos as (
    select * from {{ ref('bld_ef3__choose_stu_demos') }}
),
stu_immutable_demos as (
    select * from {{ ref('bld_ef3__immutable_stu_demos') }}
),
stu_ids as (
    select * from {{ ref('bld_ef3__wide_ids_student') }}
),
stu_chars as (
    select * from {{ ref('bld_ef3__student_characteristics') }}
),
stu_indicators as (
    select * from {{ ref('bld_ef3__student_indicators') }}
),
stu_programs as (
    select * from {{ ref('bld_ef3__student_programs') }}
),
stu_grade as (
    select * from {{ ref('bld_ef3__stu_grade_level') }}
),

-- student programs
{% if var('src:program:special_ed:enabled', True) %}
    stu_special_ed as (
        select * from {{ ref('bld_ef3__student_program__special_education') }}
    ),
{% endif %}

{% if var('src:program:language_instruction:enabled', True) %}
    stu_language_instruction as (
        select * from {{ ref('bld_ef3__student_program__language_instruction') }}
    ),
{% endif %}

{% if var('src:program:homeless:enabled', True) %}
    stu_homeless as (
        select * from {{ ref('bld_ef3__student_program__homeless') }}
    ),
{% endif %}

{% if var('src:program:title_i:enabled', True) %}
    stu_title_i_part_a as (
        select * from {{ ref('bld_ef3__student_program__title_i_part_a') }}
    ),
{% endif %}

formatted as (
    select
        stg_student.k_student,
        stg_student.k_student_xyear,
        stg_student.tenant_code,
        stg_student.api_year as school_year,
        stg_student.student_unique_id,
        -- student ids
        {{ accordion_columns(
            source_table='bld_ef3__wide_ids_student',
            exclude_columns=['tenant_code', 'api_year', 'k_student', 'k_student_xyear', 'ed_org_id'],
            source_alias='stu_ids'
        ) }}
        stu_immutable_demos.first_name,
        stu_immutable_demos.middle_name,
        stu_immutable_demos.last_name,
        stu_immutable_demos.display_name,
        stu_immutable_demos.birth_date,
        stu_demos.lep_code,
        stu_immutable_demos.gender,
        stu_grade.entry_grade_level as grade_level,
        stu_grade.grade_level_integer,
        stu_immutable_demos.race_ethnicity,
        stu_immutable_demos.has_hispanic_latino_ethnicity,

        -- student programs
        {% if var('src:program:special_ed:enabled', True) %}
            {% for agg_type in var('edu:special_ed:agg_types') %}
                coalesce(stu_special_ed.is_special_education_{{agg_type}}, false) as is_special_education_{{agg_type}},
            {% endfor %}
            {% if custom_special_ed_program_agg_indicators -%}
                {% for custom_indicator in custom_special_ed_program_agg_indicators %}
                coalesce(stu_special_ed.{{custom_indicator}}, false) as {{custom_indicator}},
                {% endfor %}
            {% endif %}
        {% endif %}

        {% if var('src:program:language_instruction:enabled', True) %}
            {% for agg_type in var('edu:language_instruction:agg_types') %}
                coalesce(stu_language_instruction.is_english_language_learner_{{agg_type}}, false) as is_english_language_learner_{{agg_type}},
            {% endfor %}
            {% if custom_language_instruction_program_agg_indicators -%}
                {% for custom_indicator in custom_language_instruction_program_agg_indicators %}
                coalesce(stu_language_instruction.{{custom_indicator}}, false) as {{custom_indicator}},
                {% endfor %}
            {% endif %}
        {% endif %}

        {% if var('src:program:homeless:enabled', True) %}
            {% for agg_type in var('edu:homeless:agg_types') %}
                coalesce(stu_homeless.is_homeless_{{agg_type}}, false) as is_homeless_{{agg_type}},
            {% endfor %}
            {% if custom_homeless_program_agg_indicators -%}
                {% for custom_indicator in custom_homeless_program_agg_indicators %}
                coalesce(stu_homeless.{{custom_indicator}}, false) as {{custom_indicator}},
                {% endfor %}
            {% endif %}
        {% endif %}

        {% if var('src:program:title_i:enabled', True) %}
            {% for agg_type in var('edu:title_i:agg_types') %}
                coalesce(stu_title_i_part_a.is_title_i_{{agg_type}}, false) as is_title_i_{{agg_type}},
            {% endfor %}
            {% if custom_title_i_program_agg_indicators -%}
                {% for custom_indicator in custom_title_i_program_agg_indicators %}
                coalesce(stu_title_i_part_a.{{custom_indicator}}, false) as {{custom_indicator}},
                {% endfor %}
            {% endif %}
        {% endif %}

        -- student characteristics
        {{ accordion_columns(
            source_table='bld_ef3__student_characteristics',
            exclude_columns=['tenant_code', 'api_year', 'k_student', 'k_student_xyear', 'ed_org_id'],
            source_alias='stu_chars',
            coalesce_value = 'FALSE'
        ) }}

        -- student indicators
        {{ accordion_columns(
            source_table='bld_ef3__student_indicators',
            exclude_columns=['tenant_code', 'api_year', 'k_student', 'k_student_xyear', 'ed_org_id'],
            source_alias='stu_indicators'
        ) }}

        -- student programs (those which do not have individual tables)
        {{ accordion_columns(
            source_table='bld_ef3__student_programs',
            exclude_columns=['tenant_code', 'api_year', 'k_student', 'k_student_xyear', 'ed_org_id'],
            source_alias='stu_programs',
            coalesce_value = 'FALSE'
        ) }}

        -- intersection groups
        {% set intersection_vars = var("edu:stu_demos:intersection_groups") %}
        {%- if intersection_vars is not none and intersection_vars | length -%}
          {%- for var in intersection_vars -%}
            {{ intersection_vars[var]['where']}} as {{ var }},
          {%- endfor -%}
        {%- endif %}

        -- custom indicators
        {% if custom_data_sources is not none and custom_data_sources | length -%}
          {%- for source in custom_data_sources -%}
            {%- for indicator in custom_data_sources[source] -%}
              {{ custom_data_sources[source][indicator]['where'] }} as {{ indicator }},
            {%- endfor -%}
          {%- endfor -%}
        {%- endif %}

        -- add indicator of most recent demographic entry
        stg_student.api_year = max(stg_student.api_year) over(partition by stg_student.k_student_xyear) as is_latest_record,
       
        stu_immutable_demos.race_array,
        stu_immutable_demos.safe_display_name

    from stg_student

    {% if var('edu:stu_demos:optional_in_dim_student', False) %} left {% endif %} join stu_demos
        on stg_student.k_student = stu_demos.k_student
    {% if var('edu:stu_demos:optional_in_dim_student', False) %} left {% endif %} join stu_immutable_demos
        on stu_demos.{{demos_join_var}} = stu_immutable_demos.{{demos_join_var}}
        and stu_demos.ed_org_id = stu_immutable_demos.ed_org_id
    left join stu_ids
        on stu_demos.k_student = stu_ids.k_student
        and stu_demos.ed_org_id = stu_ids.ed_org_id
    left join stu_chars
        on stu_demos.k_student = stu_chars.k_student
        and stu_demos.ed_org_id = stu_chars.ed_org_id
    left join stu_indicators
        on stu_demos.k_student = stu_indicators.k_student
        and stu_demos.ed_org_id = stu_indicators.ed_org_id
    left join stu_programs
        on stu_demos.k_student = stu_programs.k_student
        and stu_demos.ed_org_id = stu_programs.ed_org_id
    left join stu_grade
        on stu_demos.k_student = stu_grade.k_student
        and stg_student.api_year = stu_grade.school_year

    -- student programs
    {% if var('src:program:special_ed:enabled', True) %}
        left join stu_special_ed
            on stu_demos.k_student = stu_special_ed.k_student
    {% endif %}

    {% if var('src:program:language_instruction:enabled', True) %}
        left join stu_language_instruction
            on stu_demos.k_student = stu_language_instruction.k_student
    {% endif %}

    {% if var('src:program:homeless:enabled', True) %}
        left join stu_homeless
            on stu_demos.k_student = stu_homeless.k_student
    {% endif %}

    {% if var('src:program:title_i:enabled', True) %}
        left join stu_title_i_part_a
            on stu_demos.k_student = stu_title_i_part_a.k_student
    {% endif %}

    -- custom data sources
    -- Note, dbt test "custom_demo_sources_are_unique_on_k_student" is configured to fail if any not unique by k_student
    {% if custom_data_sources is not none and custom_data_sources | length -%}
      {%- for source in custom_data_sources -%}
        left join {{ ref(source) }}
          on stu_demos.k_student = {{ source }}.k_student
      {% endfor %}
    {%- endif %}

)

select * from formatted
order by tenant_code, school_year desc, k_student
