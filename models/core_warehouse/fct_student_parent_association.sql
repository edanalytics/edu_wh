{{
  config(
    post_hook=[
        "alter table {{ this }} alter column k_student set not null",
        "alter table {{ this }} alter column k_parent set not null",
        "alter table {{ this }} add primary key (k_student, k_parent)",
        "alter table {{ this }} add constraint fk_{{ this.name }}_student foreign key (k_student) references {{ ref('dim_student') }}",
        "alter table {{ this }} add constraint fk_{{ this.name }}_parent foreign key (k_parent) references {{ ref('dim_parent') }}",
    ]
  )
}}

{% set custom_data_sources_name = "edu:student_parent_association:custom_data_sources" %}

with stg_stu_parent as (
    -- parents were renamed to contacts in Data Standard v5.0
    -- the contacts staging tables contain both parent and contact records
    select * from {{ ref('stg_ef3__student_contact_associations') }}
),
dim_student as (
    select * from {{ ref('dim_student') }}
),
dim_parent as (
    select * from {{ ref('dim_parent') }}
),
-- the goal here is to find the most recent student record
most_recent_k_student as (
    select k_student
    from dim_student
    qualify school_year = max(school_year) over (partition by k_student_xyear) 
),
formatted as (
    select 
        dim_student.k_student,
        dim_student.k_student_xyear,
        dim_parent.k_parent,
        stg_stu_parent.tenant_code,
        stg_stu_parent.school_year,
        stg_stu_parent.contact_priority,
        stg_stu_parent.contact_restrictions,
        stg_stu_parent.relation_type,
        stg_stu_parent.is_emergency_contact,
        stg_stu_parent.is_living_with,
        stg_stu_parent.is_primary_contact,
        stg_stu_parent.is_legal_guardian
        {# add any extension columns configured from stg_ef3__student_contact_associations #}
        {{ edu_edfi_source.extract_extension(model_name='stg_ef3__student_contact_associations', flatten=False) }}

        -- custom data sources columns
        {{ add_cds_columns(cds_model_config=custom_data_sources_name) }}
    from stg_stu_parent
    -- subset to only the stu/parent records associated with the most recent student records
    join most_recent_k_student
        on stg_stu_parent.k_student = most_recent_k_student.k_student
    -- this will associate the above stu/parent records to all student records (aka. all k_student for a k_student_xyear)
    join dim_student 
        on stg_stu_parent.k_student_xyear = dim_student.k_student_xyear
    join dim_parent
        on stg_stu_parent.k_contact = dim_parent.k_parent
        
    -- custom data sources
    {{ add_cds_joins_v2(cds_model_config=custom_data_sources_name) }}
)
select * from formatted