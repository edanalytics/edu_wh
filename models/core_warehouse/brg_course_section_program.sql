{{
  config(
    post_hook=[
        "alter table {{ this }} alter column k_course_section set not null",
        "alter table {{ this }} alter column k_program set not null",
        "alter table {{ this }} add primary key (k_course_section, k_program)",
        "alter table {{ this }} add constraint fk_{{ this.name }}_course_section foreign key (k_course_section) references {{ ref('dim_course_section') }}",
        "alter table {{ this }} add constraint fk_{{ this.name }}_program foreign key (k_program) references {{ ref('dim_program') }}"
    ]
  )
}}
  
with sections as (
    select * from {{ ref('dim_course_section') }}
),
stg_ef3__sections__programs as (
    select * from {{ ref('stg_ef3__sections__programs') }}
),
dim_program as (
    select * from {{ ref('dim_program') }}
),
joined as (
    select 
        stg_ef3__sections__programs.k_course_section,
        stg_ef3__sections__programs.k_program
    from stg_ef3__sections__programs
    join sections
        on stg_ef3__sections__programs.k_course_section = sections.k_course_section
    join dim_program 
        on stg_ef3__sections__programs.k_program = dim_program.k_program
)
select * from joined