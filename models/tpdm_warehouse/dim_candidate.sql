{{
  config(
    post_hook=[
        "alter table {{ this }} add primary key (k_candidate)"
    ]
  )
}}

{# If edu var has been configured to make demos immutable, set join var to `k_candidate_xyear` bc demos are unique by xyear #}
{# otherwise, use k_candidate bc demos are unique by candidate+year #}
{%- if var('edu:stu_demos:make_demos_immutable', False) -%}
    {%- set demos_join_var = 'k_candidate_xyear' -%}
{%- else -%}
    {%- set demos_join_var = 'k_candidate' -%}
{%- endif -%}

{% set other_name_types = var('edu:candidate_demos:other_names', None) %}
{%- set name_type_list = ['personal_title_prefix', 'first_name', 'middle_name', 'last_surname', 'generation_code_suffix']-%}

with stg_candidates as (
    select * from {{ ref('stg_tpdm__candidates') }}
),

candidate_immutable_demos as (
    select * from {{ ref('bld_tpdm__immutable_candidate_demos') }}
),
candidate_other_names as (
    select * from {{ ref('bld_tpdm__candidate__other_names') }}
),

formatted as (
    select
        stg_candidates.k_candidate,
        stg_candidates.k_person,
        stg_candidates.tenant_code,
        stg_candidates.school_year,
        stg_candidates.candidate_id,
        stg_candidates.person_id,
        candidate_immutable_demos.first_name,
        candidate_immutable_demos.last_name,
        candidate_immutable_demos.middle_name,
        candidate_immutable_demos.maiden_name,
        candidate_immutable_demos.preferred_first_name,
        candidate_immutable_demos.preferred_last_name,
        stg_candidates.birth_city,
        stg_candidates.birth_date,
        stg_candidates.birth_international_province,
        stg_candidates.is_economic_disadvantaged,
        stg_candidates.is_first_generation_student,
        candidate_immutable_demos.race_ethnicity,
        candidate_immutable_demos.has_hispanic_latino_ethnicity,
        stg_candidates.is_multiple_birth,
        candidate_immutable_demos.gender,
        stg_candidates.birth_state,
        stg_candidates.birth_country,
        stg_candidates.english_language_exam,
        stg_candidates.lep_code,
        stg_candidates.v_disabilities,
        stg_candidates.v_languages,
        stg_candidates.v_other_names,
        stg_candidates.v_personal_identification_documents,

                -- other name types
        {% if other_name_types is not none and other_name_types | length -%}    
            {%- for type in other_name_types -%}
                {%- for name_type in name_type_list -%}
                        candidate_other_names.{{dbt_utils.slugify(type)}}_{{name_type}},
                {%- endfor -%}
            {%- endfor -%}
        {%- endif -%}

        candidate_immutable_demos.race_array,
        candidate_immutable_demos.safe_display_name

    from stg_candidates

    join candidate_immutable_demos
      on stg_candidates.k_candidate = candidate_immutable_demos.k_candidate
    left join candidate_other_names
      on stg_candidates.k_candidate = candidate_other_names.k_candidate
)

select * from formatted