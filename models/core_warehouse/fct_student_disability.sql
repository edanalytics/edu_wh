{{
  config(
    post_hook=[
        "alter table {{ this }} alter column k_student_disability set not null",
        "alter table {{ this }} alter column k_student set not null",
        "alter table {{ this }} alter column disability_type set not null",
        "alter table {{ this }} add primary key (k_student_disability)",
        "alter table {{ this }} add constraint fk_{{ this.name }}_student foreign key (k_student) references {{ ref('dim_student') }}",
        "alter table {{ this }} add constraint fk_{{ this.name }}_program foreign key (k_program) references {{ ref('dim_program') }}",
        "alter table {{ this }} add constraint fk_{{ this.name }}_lea foreign key (k_lea) references {{ ref('dim_lea') }}",
        "alter table {{ this }} add constraint fk_{{ this.name }}_school foreign key (k_school) references {{ ref('dim_school') }}",
    ]
  )
}}

{{ cds_depends_on('edu:student_disability:custom_data_sources') }}
{% set custom_data_sources = var('edu:student_disability:custom_data_sources', []) %}

with student_disabilities as (
    select * from {{ ref('bld_ef3__student__disabilities') }}
),
dim_student as (
    select * from {{ ref('dim_student') }}
),
student_disability_designations as (
    select * from {{ ref('bld_ef3__student__wide_disability_designations') }}
),
formatted as (
    select
        {{ dbt_utils.generate_surrogate_key(
            ['sd.tenant_code',
            'sd.school_year',
            'sd.k_student',
            'sd.k_lea',
            'sd.k_school',
            'sd.k_program',
            'sd.ed_org_id',
            'sd.program_enroll_begin_date',]
        ) }} as k_student_disability, 
        sd.k_student,
        stu.k_student_xyear,
        sd.k_lea,
        sd.k_school,
        sd.k_program,
        sd.ed_org_id,
        sd.program_enroll_begin_date,
        sd.program_enroll_end_date,
        sd.tenant_code,
        sd.api_year,
        sd.school_year,
        sd.disability_type,
        sd.disability_source_type,
        sd.disability_diagnosis,
        sd.order_of_disability,
        -- disability designations
        {{ accordion_columns(
            source_table='bld_ef3__student__wide_disability_designations',
            exclude_columns=['tenant_code', 'api_year', 'school_year', 'k_student', 'ed_org_id', 'k_lea', 'k_school', 'k_program', 'disability_type'],
            source_alias='disability_designations',
            add_trailing_comma=false
        ) }}
    from student_disabilities sd
    join dim_student stu
        on stu.k_student = sd.k_student
    left join student_disability_designations disability_designations
        on sd.k_student = disability_designations.k_student
        and (sd.k_lea = disability_designations.k_lea or (sd.k_lea is null and disability_designations.k_lea is null))
        and (sd.k_school = disability_designations.k_school or (sd.k_school is null and disability_designations.k_school is null))
        and (sd.k_program = disability_designations.k_program or (sd.k_program is null and disability_designations.k_program is null))
        and sd.ed_org_id = disability_designations.ed_org_id
        and sd.tenant_code = disability_designations.tenant_code
        and sd.school_year = disability_designations.school_year
        and sd.disability_type = disability_designations.disability_type
),
cds_cols as (
    select
        f.*

        -- custom data sources columns
        {{ add_cds_columns(custom_data_sources=custom_data_sources) }}
    from formatted f

    -- custom data sources
    {{ add_cds_joins_v1(custom_data_sources=custom_data_sources, driving_alias='f', join_cols=['k_student_disability']) }}
    {{ add_cds_joins_v2(custom_data_sources=custom_data_sources) }}
)
select * from cds_cols