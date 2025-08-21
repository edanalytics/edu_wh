{{
  config(
    post_hook=[
        "alter table {{ this }} add primary key (k_student, k_program, program_enroll_begin_date, program_service)",
        "alter table {{ this }} add constraint fk_{{ this.name }}_student foreign key (k_student) references {{ ref('dim_student') }}",
        "alter table {{ this }} add constraint fk_{{ this.name }}_program foreign key (k_program) references {{ ref('dim_program') }}",
    ]
  )
}}


-- Define all optional program service models here.
{% set stage_program_relations = [] %}

-- Special Education
{% if var('src:program:special_ed:enabled', True) %}
    {% do stage_program_relations.append(ref('stg_ef3__stu_spec_ed__program_services')) %}
{% endif %}

-- Language Instruction
{% if var('src:program:language_instruction:enabled', True) %}
    {% do stage_program_relations.append(ref('stg_ef3__stu_lang_instr__program_services')) %}
{% endif %}

-- Homeless
{% if var('src:program:homeless:enabled', True) %}
    {% do stage_program_relations.append(ref('stg_ef3__stu_homeless__program_services')) %}
{% endif %}

-- Title I Part A
{% if var('src:program:title_i:enabled', True) %}
    {% do stage_program_relations.append(ref('stg_ef3__stu_title_i_part_a__program_services')) %}
{% endif %}

-- CTE
{% if var('src:program:cte:enabled', True) %}
    {% do stage_program_relations.append(ref('stg_ef3__stu_cte__program_services')) %}
{% endif %}

-- Migrant Education
{% if var('src:program:migrant_education:enabled', True) %}
    {% do stage_program_relations.append(ref('stg_ef3__stu_migrant_edu__program_services')) %}
{% endif %}

-- Food Service
{% if var('src:program:food_service:enabled', True) %}
    {% do stage_program_relations.append(ref('stg_ef3__stu_school_food_service__program_services')) %}
{% endif %}

with dim_student as (
    select * from {{ ref('dim_student') }}
),

dim_program as (
    select * from {{ ref('dim_program') }}
),

stacked as (
    {{ dbt_utils.union_relations(

        relations=stage_program_relations

    ) }}
),

{# -- append the name of each `stage_program_relations` into a new list, so we can pass that list to `extract_extension` below -- #}
{% set relation_names = [] -%}
{%- for relation in stage_program_relations -%}
  {%- do relation_names.append(relation.name) -%}
{%- endfor -%}

subset as (
  select
    stacked.k_student,
    stacked.k_student_xyear,
    stacked.k_program,
    stacked.tenant_code,
    stacked.program_enroll_begin_date,
    stacked.program_service,
    stacked.primary_indicator,
    stacked.v_providers,
    stacked.service_begin_date,
    stacked.service_end_date
    {# add any extension columns configured from all stage_program_relations #}
    {{ edu_edfi_source.extract_extension(model_name=relation_names, flatten=False) }}

  from stacked
  join dim_program
    on stacked.k_program = dim_program.k_program
)

select * from subset
