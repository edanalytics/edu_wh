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
    {% do stage_program_relations.append('stg_ef3__stu_spec_ed__program_services') %}
{% endif %}

-- Language Instruction
{% if var('src:program:language_instruction:enabled', True) %}
    {% do stage_program_relations.append('stg_ef3__stu_lang_instr__program_services') %}
{% endif %}

-- Homeless
{% if var('src:program:homeless:enabled', True) %}
    {% do stage_program_relations.append('stg_ef3__stu_homeless__program_services') %}
{% endif %}

-- Title I Part A
{% if var('src:program:title_i:enabled', True) %}
    {% do stage_program_relations.append('stg_ef3__stu_title_i_part_a__program_services') %}
{% endif %}


with dim_student as (
    select * from {{ ref('dim_student') }}
),

dim_program as (
    select * from {{ ref('dim_program') }}
),

stacked as (
    {% for relation in stage_program_relations %}
        select
            stage.k_student,
            stage.k_student_xyear,
            stage.k_program,
            stage.tenant_code,
            stage.program_enroll_begin_date,
            stage.program_service,
            stage.primary_indicator,
            stage.v_providers,
            stage.service_begin_date,
            stage.service_end_date
            {{ edu_edfi_source.extract_extension(model_name=relation, flatten=False) }}

        from {{ ref(relation) }} as stage

            join dim_program
                on stage.k_program = dim_program.k_program

        {% if not loop.last %} union all {% endif %}
    {% endfor %}
)

select * from stacked
