{{
  config(
    post_hook=[
        "alter table {{ this }} alter column k_staff set not null",
        "alter table {{ this }} alter column k_course_section set not null",
        "alter table {{ this }} alter column begin_date set not null",
        "alter table {{ this }} add primary key (k_staff, k_course_section, begin_date)",
        "alter table {{ this }} add constraint fk_{{ this.name }}_staff foreign key (k_staff) references {{ ref('dim_staff') }}",
        "alter table {{ this }} add constraint fk_{{ this.name }}_section foreign key (k_course_section) references {{ ref('dim_course_section') }}",
    ]
  )
}}

{{ cds_depends_on('edu:staff_section_association:custom_data_sources') }}
{% set custom_data_sources = var('edu:staff_section_association:custom_data_sources', []) %}

with stg_staff_section as (
    select * from {{ ref('stg_ef3__staff_section_associations') }}
),
dim_staff as (
    select * from {{ ref('dim_staff') }}
),
dim_course_section as (
    select * from {{ ref('dim_course_section') }}
),
formatted as (
    select
        dim_staff.k_staff,
        dim_course_section.k_school,
        dim_course_section.k_course_section,
        stg_staff_section.tenant_code,
        stg_staff_section.school_year,
        stg_staff_section.begin_date,
        stg_staff_section.end_date,
        stg_staff_section.classroom_position,
        stg_staff_section.is_highly_qualified_teacher,
        stg_staff_section.percentage_contribution,
        stg_staff_section.teacher_student_data_link_exclusion,
        -- create indicator for active assignment
        iff(
            -- is highest school year observed by tenant
            stg_staff_section.school_year = max(stg_staff_section.school_year)
                over (partition by stg_staff_section.tenant_code)
            -- not yet exited
            and (stg_staff_section.end_date is null
                or stg_staff_section.end_date >= current_date())
            -- assignment has begun
            and stg_staff_section.begin_date <= current_date(),
            true, false
        ) as is_active_assignment
        {# add any extension columns configured from stg_ef3__staff_section_associations #}
        {{ edu_edfi_source.extract_extension(model_name='stg_ef3__staff_section_associations', flatten=False) }}

        -- custom data sources columns
        {{ add_cds_columns(custom_data_sources=custom_data_sources) }}
    from stg_staff_section
    join dim_staff
        on stg_staff_section.k_staff = dim_staff.k_staff
    join dim_course_section
        on stg_staff_section.k_course_section = dim_course_section.k_course_section
        
    -- custom data sources
    {{ add_cds_joins_v1(custom_data_sources=custom_data_sources, driving_alias='stg_staff_section', join_cols=['k_staff', 'k_course_section', 'begin_date']) }}
    {{ add_cds_joins_v2(custom_data_sources=custom_data_sources) }}
)
select * from formatted
