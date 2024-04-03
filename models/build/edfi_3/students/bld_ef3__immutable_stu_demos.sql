{# If edu var has been configured to make demos immutable, set partition var to `k_student_xyear` so demos are unique by xyear #}
{# otherwise, use k_student so demos are unique by student+year #}
{%- if var('edu:stu_demos:make_demos_immutable', False) -%}
    {%- set stu_partition_var = 'k_student_xyear' -%}
{%- else -%}
    {%- set stu_partition_var = 'k_student' -%}
{%- endif -%}

-- extract the most recent version of immutable student demographics
-- so that our most current understanding of these values can be applied
-- accross all historic years
with stg_student as (
    select * from {{ ref('stg_ef3__students') }}
),
stu_demos as (
    select * from {{ ref('bld_ef3__choose_stu_demos') }}
),
stu_races as (
    select * from {{ ref('bld_ef3__stu_race_ethnicity') }}
),
joined as (
    select
        stg_student.k_student,
        stg_student.k_student_xyear,
        stg_student.tenant_code,
        stg_student.api_year as school_year,
        stu_demos.ed_org_id,
        stg_student.first_name,
        stg_student.middle_name,
        stg_student.last_name,
        {# stu_display_name logic: prefer SQL from this dbt variable, but default to "concat(...)" #}
        {{ var('edu:stu_demos:display_name_sql',
          "concat(
            stg_student.last_name, ', ',
            stg_student.first_name,
            coalesce(' ' || left(stg_student.middle_name, 1), '')
            )"
        }} as display_name,
        concat(display_name, ' (', stg_student.student_unique_id, ')') as safe_display_name,
        stg_student.birth_date,
        stu_demos.gender,
        stu_races.race_ethnicity,
        stu_races.race_array
    from stg_student
    join stu_demos
        on stg_student.k_student = stu_demos.k_student
    left join stu_races
        on stu_demos.k_student = stu_races.k_student
        and stu_demos.ed_org_id = stu_races.ed_org_id
),
deduped as (
        {{
        dbt_utils.deduplicate(
            relation='joined',
                partition_by=stu_partition_var,
            order_by='school_year desc'
        )
    }}
)
select * from deduped
