{{
  config(
    post_hook=[
        "alter table {{ this }} add primary key (k_student, k_parent)",
        "alter table {{ this }} add constraint fk_{{ this.name }}_student foreign key (k_student) references {{ ref('dim_student') }}",
        "alter table {{ this }} add constraint fk_{{ this.name }}_parent foreign key (k_parent) references {{ ref('dim_parent') }}",
    ]
  )
}}

with stg_stu_parent as (
    select * from {{ ref('stg_ef3__student_parent_associations') }}
),
dim_student as (
    select * from {{ ref('dim_student') }}
),
dim_parent as (
    select * from {{ ref('dim_parent') }}
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
    from stg_stu_parent
    join dim_student 
        on stg_stu_section.k_student = dim_student.k_student
    join dim_parent
        on stg_stu_parent.k_parent = dim_parent.k_parent
)
select * from formatted