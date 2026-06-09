-- Define all optional disability models here.
{% set stage_disability_relations = [] %}

-- Ed Org Disabilities
{% do stage_disability_relations.append(ref('stg_ef3__stu_ed_org__disabilities')) %}

-- Special Education
{% if var('src:program:special_ed:enabled', True) %}
    {% do stage_disability_relations.append(ref('stg_ef3__stu_spec_ed__disabilities')) %}
{% endif %}

with stacked as (
    {{ dbt_utils.union_relations(
        relations=stage_disability_relations
    ) }}
),
formatted as (
    select
        tenant_code,
        api_year,
        school_year,
        k_student,
        k_lea,
        k_school,
        k_program,
        k_student_program,
        program_enroll_begin_date,
        program_enroll_end_date,
        disability_type,
        disability_source_type,
        disability_diagnosis,
        order_of_disability,
        v_designations,
        -- if k_program is populated, this is a program disability; otherwise it is an ed org disability.
        k_program is not null as is_program
    from stacked
)
select * from formatted
