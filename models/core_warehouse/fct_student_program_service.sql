{{
  config(
    post_hook=[
        "alter table {{ this }} add primary key (k_student, k_program, program_begin_date, program_service)",
        "alter table {{ this }} add constraint fk_{{ this.name }}_student foreign key (k_student) references {{ ref('dim_student') }}",
        "alter table {{ this }} add constraint fk_{{ this.name }}_program foreign key (k_program) references {{ ref('dim_program') }}",
    ]
  )
}}

with dim_student as (
    select * from {{ ref('dim_student') }}
),

dim_program as (
    select * from {{ ref('dim_program') }}
),


-- Build all optional program service models here.
{% set program_select_ctes = [] %}

{% if var('src:program:special_ed:enabled', True) %}
    stg_stu_spec_ed_services as (
        select * from {{ ref('stg_ef3__stu_spec_ed__program_services') }}
    ),

    spec_ed_select as (
        select
            stg_stu_spec_ed_services.k_student,
            stg_stu_spec_ed_services.k_program,
            stg_stu_spec_ed_services.tenant_code,
            stg_stu_spec_ed_services.spec_ed_program_begin_date as program_begin_date,
            stg_stu_spec_ed_services.special_education_program_service as program_service,
            stg_stu_spec_ed_services.primary_indicator,
            stg_stu_spec_ed_services.v_providers,
            stg_stu_spec_ed_services.service_begin_date,
            stg_stu_spec_ed_services.service_end_date
            {{ edu_edfi_source.extract_extension(model_name='stg_ef3__stu_spec_ed__program_services', flatten=False) }}

        from stg_stu_spec_ed_services
            join dim_program
                on stg_stu_spec_ed_services.k_program = dim_program.k_program
    ),

    {% do program_select_ctes.append('spec_ed_select') %}
{% endif %}

{% if var('src:program:language_instruction:enabled', True) %}
    stg_stu_lang_instr_services as (
        select * from {{ ref('stg_ef3__stu_lang_instr__program_services') }}
    ),

    lang_instr_select as (
        select
            stg_stu_lang_instr_services.k_student,
            stg_stu_lang_instr_services.k_program,
            stg_stu_lang_instr_services.tenant_code,
            stg_stu_lang_instr_services.program_enroll_begin_date as program_begin_date,
            stg_stu_lang_instr_services.language_instruction_program_service as program_service,
            stg_stu_lang_instr_services.primary_indicator,
            stg_stu_lang_instr_services.v_providers,
            stg_stu_lang_instr_services.service_begin_date,
            stg_stu_lang_instr_services.service_end_date
            {{ edu_edfi_source.extract_extension(model_name='stg_ef3__stu_lang_instr__program_services', flatten=False) }}

        from stg_stu_lang_instr_services
            join dim_program
                on stg_stu_lang_instr_services.k_program = dim_program.k_program
    ),

    {% do program_select_ctes.append('lang_instr_select') %}
{% endif %}

{% if var('src:program:homeless:enabled', True) %}
    stg_stu_homeless_services as (
        select * from {{ ref('stg_ef3__stu_homeless__program_services') }}
    ),

    homeless_select as (
        select
            stg_stu_homeless_services.k_student,
            stg_stu_homeless_services.k_program,
            stg_stu_homeless_services.tenant_code,
            stg_stu_homeless_services.program_enroll_begin_date as program_begin_date,
            stg_stu_homeless_services.homeless_program_service as program_service,
            stg_stu_homeless_services.primary_indicator,
            stg_stu_homeless_services.v_providers,
            stg_stu_homeless_services.service_begin_date,
            stg_stu_homeless_services.service_end_date
            {{ edu_edfi_source.extract_extension(model_name='stg_ef3__stu_homeless__program_services', flatten=False) }}

        from stg_stu_homeless_services
            join dim_program
                on stg_stu_homeless_services.k_program = dim_program.k_program
    ),

    {% do program_select_ctes.append('homeless_select') %}
{% endif %}

{% if var('src:program:title_i:enabled', True) %}
    stg_stu_title_i_services as (
        select * from {{ ref('stg_ef3__stu_title_i_part_a__program_services') }}
    ),

    title_i_select as (
        select
            stg_stu_title_i_services.k_student,
            stg_stu_title_i_services.k_program,
            stg_stu_title_i_services.tenant_code,
            stg_stu_title_i_services.program_enroll_begin_date as program_begin_date,
            stg_stu_title_i_services.title_i_part_a_program_service as program_service,
            stg_stu_title_i_services.primary_indicator,
            stg_stu_title_i_services.v_providers,
            stg_stu_title_i_services.service_begin_date,
            stg_stu_title_i_services.service_end_date
            {{ edu_edfi_source.extract_extension(model_name='stg_ef3__stu_title_i_part_a__program_services', flatten=False) }}

        from stg_stu_title_i_services
            join dim_program
                on stg_stu_title_i_services.k_program = dim_program.k_program
    ),

    {% do program_select_ctes.append('title_i_select') %}
{% endif %}

stacked as (
    {% for cte in program_select_ctes %}
        select * from {{ cte }}
        {% if not loop.last %} union all {% endif %}
    {% endfor %}
)

select * from stacked
