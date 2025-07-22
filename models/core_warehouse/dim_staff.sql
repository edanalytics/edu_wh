{{
  config(
    post_hook=[
        "alter table {{ this }} alter column k_staff set not null",
        "alter table {{ this }} add primary key (k_staff)",
    ]
  )
}}
{# Load custom data sources from var #}
{% set custom_data_sources = var("edu:staff:custom_data_sources", []) %}

with stg_staff as (
    select * from {{ ref('stg_ef3__staffs') }}
),
bld_ef3__wide_ids_staff as (
    select * from {{ ref('bld_ef3__wide_ids_staff') }}
),
staff_race_ethnicity as (
    select * from {{ ref('bld_ef3__staff_race_ethnicity') }}
),
choose_email as (
    {{ row_pluck(ref('bld_ef3__staff_emails'),
                key='k_staff',
                column='email_type',
                preferred=var('edu:staff:preferred_email', 'Work')
                ) }}
),
-- emails
formatted as (
    select 
        stg_staff.k_staff,
        stg_staff.tenant_code,
        stg_staff.staff_unique_id,
        {{ accordion_columns(
            source_table='bld_ef3__wide_ids_staff', 
            exclude_columns=['tenant_code', 'api_year', 'k_staff']) }}
        stg_staff.login_id,
        choose_email.email_address,
        choose_email.email_type,
        stg_staff.last_name || ', ' || stg_staff.first_name as display_name,
        stg_staff.first_name,
        stg_staff.last_name,
        stg_staff.middle_name,
        stg_staff.personal_title_prefix,
        stg_staff.generation_code_suffix,
        stg_staff.preferred_first_name,
        stg_staff.preferred_last_name,
        stg_staff.birth_date,
        stg_staff.gender,
        stg_staff.gender_identity,
        staff_race_ethnicity.race_ethnicity,
        stg_staff.highest_completed_level_of_education,
        stg_staff.is_highly_qualified_teacher,
        stg_staff.years_of_prior_professional_experience,
        stg_staff.years_of_prior_teaching_experience

        -- custom indicators
        {% if custom_data_sources is not none and custom_data_sources | length -%}
          {%- for source in custom_data_sources -%}
            {%- for indicator in custom_data_sources[source] -%}
              , {{ custom_data_sources[source][indicator]['where'] }} as {{ indicator }}
            {%- endfor -%}
          {%- endfor -%}
        {%- endif %}
    from stg_staff
    left join bld_ef3__wide_ids_staff 
        on stg_staff.k_staff = bld_ef3__wide_ids_staff.k_staff
    left join choose_email
        on stg_staff.k_staff = choose_email.k_staff
    left join staff_race_ethnicity
        on stg_staff.k_staff = staff_race_ethnicity.k_staff
    -- custom data sources
    {% if custom_data_sources is not none and custom_data_sources | length -%}
      {%- for source in custom_data_sources -%}
        left join {{ ref(source) }}
          on stg_staff.k_staff = {{ source }}.k_staff
      {% endfor %}
    {%- endif %}
)
select * from formatted
order by tenant_code, k_staff
