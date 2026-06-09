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
        -- Generated here so bld_ef3__student__wide_disability_designations can carry it forward,
        -- allowing fct_student_disability to join on a single key rather than a null-safe multi-column join.
        {{ dbt_utils.generate_surrogate_key([
            'tenant_code',
            'school_year',
            'k_student',
            'k_lea',
            'k_school',
            'k_program',
            'program_enroll_begin_date',
            'disability_type',
        ]) }} as k_student_disability,
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
