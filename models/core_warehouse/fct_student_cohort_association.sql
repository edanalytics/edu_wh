{{
  config(
    post_hook=[
        "alter table {{ this }} alter column k_student set not null",
        "alter table {{ this }} alter column k_cohort set not null",
        "alter table {{ this }} alter column cohort_begin_date set not null",
        "alter table {{ this }} add primary key (k_student, k_cohort, cohort_begin_date)",
        "alter table {{ this }} add constraint fk_{{ this.name }}_student foreign key (k_student) references {{ ref('dim_student') }}",
        "alter table {{ this }} add constraint fk_{{ this.name }}_cohort foreign key (k_cohort) references {{ ref('dim_cohort') }}",
    ]
  )
}}

{{ cds_depends_on('edu:student_cohort_association:custom_data_sources') }}
{% set custom_data_sources = var('edu:student_cohort_association:custom_data_sources', []) %}

with stage as (
    select * from {{ ref('stg_ef3__student_cohort_associations') }}
),

dim_student as (
    select * from {{ ref('dim_student') }}
),

dim_cohort as (
    select * from {{ ref('dim_cohort') }}
),

formatted as (
    select
        dim_student.k_student,
        dim_student.k_student_xyear,
        dim_cohort.k_cohort,
        dim_cohort.k_lea,
        dim_cohort.k_school,
        stage.tenant_code,
        stage.school_year,
        stage.cohort_begin_date,
        stage.cohort_end_date,
        -- create indicator for active cohort
        iff(
            -- is highest school year observed by tenant
            stage.school_year = max(stage.school_year) 
                over(partition by stage.tenant_code)
            -- not yet exited
            and (cohort_end_date is null
                or cohort_end_date >= current_date())
            -- enrollment has begun
            and cohort_begin_date <= current_date(),
            true, false
        ) as is_active_cohort_association
        {# add any extension columns configured from stg_ef3__student_cohort_associations #}
        {{ edu_edfi_source.extract_extension(model_name='stg_ef3__student_cohort_associations', flatten=False) }}
        
        -- custom data sources columns
        {{ add_cds_columns(custom_data_sources=custom_data_sources) }}
    from stage
    inner join dim_student
        on stage.k_student = dim_student.k_student
    inner join dim_cohort
        on stage.k_cohort = dim_cohort.k_cohort
        
    -- custom data sources
    {{ add_cds_joins_v2(custom_data_sources=custom_data_sources) }}
)

select * from formatted