{{
  config(
    post_hook=[
        "alter table {{ this }} alter column k_school set not null",
        "alter table {{ this }} alter column grade_level set not null",
        "alter table {{ this }} alter column school_year set not null",
        "alter table {{ this }} add primary key (k_school, school_year, grade_level)",
        "alter table {{ this }} add constraint fk__{{ this.name }}__school foreign key (k_school) references {{ ref('dim_school') }}"
    ]
  )
}}

with stg_school_grade_levels as (
    select * from {{ ref('stg_ef3__schools__grade_levels__by_year') }}
),
xwalk_grade_levels as (
    select * from {{ ref('xwalk_grade_levels') }}
),
formatted as (
    select 
        sgl.tenant_code, 
        sgl.k_school, 
        sgl.school_year,
        sgl.grade_level,

        -- xwalk_grade_levels columns
        {{ accordion_columns(
            source_table='xwalk_grade_levels',
            exclude_columns=['grade_level'],
            source_alias='xgl',
            coalesce_value = 'null',
            add_trailing_comma = False
        ) }}

    from stg_school_grade_levels sgl
    left join xwalk_grade_levels xgl
        on xgl.grade_level = sgl.grade_level
)
select * from formatted
order by tenant_code, k_school, school_year desc