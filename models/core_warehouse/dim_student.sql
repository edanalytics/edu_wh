{{
  config(
    post_hook=[
        "alter table {{ this }} add primary key (k_student)",
    ]
  )
}}

{% set custom_data_sources = var("edu:stu_demos:custom_data_sources") %}

with stg_student as (
    select * from {{ ref('stg_ef3__students') }}
),
stu_demos as (
    select * from {{ ref('bld_ef3__choose_stu_demos') }}
),
stu_races as (
    select * from {{ ref('bld_ef3__stu_race_ethnicity') }}
),
stu_chars as (
    select * from {{ ref('bld_ef3__student_characteristics') }}
),
stu_grade as (
    select * from {{ ref('bld_ef3__stu_grade_level') }}
),
stu_annual_spec_ed as (
    select * from {{ ref('bld_ef3__student_special_education_annual') }}
),
stu_is_spec_ed as (
    select * from {{ ref('bld_ef3__student_special_education_active') }}
),
formatted as (
    select 
        stg_student.k_student,
        stg_student.k_student_xyear,
        stg_student.tenant_code,
        stg_student.api_year as school_year,
        stg_student.student_unique_id,
        stg_student.first_name,
        stg_student.middle_name,
        stg_student.last_name,
        concat(stg_student.last_name, ', ', stg_student.first_name,
            coalesce(' ' || left(stg_student.middle_name, 1), '')) as display_name,
        stg_student.birth_date,
        stu_demos.lep_code,
        stu_demos.gender,
        stu_grade.entry_grade_level as grade_level,
        stu_races.race_ethnicity,
        coalesce(stu_annual_spec_ed.is_special_education_annual, false) as is_special_education_annual,
        coalesce(stu_is_spec_ed.is_special_education_active, false) as is_special_education_active,
        {{ dbt_utils.star(ref('bld_ef3__student_characteristics'),
                          except=['tenant_code', 'api_year', 'k_student', 
                          'k_student_xyear', 'ed_org_id']) }},

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
        -- todo: bring in additional summarized attributes

        
        stu_races.race_array,
        concat(display_name, ' (', stg_student.student_unique_id, ')') as safe_display_name
    from stg_student
    join stu_demos
        on stg_student.k_student = stu_demos.k_student
    left join stu_races 
        on stu_demos.k_student = stu_races.k_student
        and stu_demos.ed_org_id = stu_races.ed_org_id
    left join stu_chars 
        on stu_demos.k_student = stu_chars.k_student
        and stu_demos.ed_org_id = stu_chars.ed_org_id
    left join stu_grade
        on stu_demos.k_student = stu_grade.k_student
        and stg_student.api_year = stu_grade.school_year
    left join stu_annual_spec_ed
        on stu_demos.k_student = stu_annual_spec_ed.k_student
    left join stu_is_spec_ed
        on stu_demos.k_student = stu_is_spec_ed.k_student
    -- custom data sources
    {% if custom_data_sources is not none and custom_data_sources | length -%}
      {%- for source in custom_data_sources -%}
        left join {{ ref(source) }}
          on stu_demos.k_student = {{ source }}.k_student
      {%- endfor -%}
    {%- endif %}

)
select * from formatted
order by tenant_code, school_year desc, k_student