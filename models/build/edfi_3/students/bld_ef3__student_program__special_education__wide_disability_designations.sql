with student_sped_disabilities as (
    select * from {{ ref('stg_ef3__stu_spec_ed__disabilities') }}
),
xwalk_disability_designations as (
    select * from {{ ref('xwalk_disability_designations') }}
),
flattened as (
    select 
        k_student,
        k_program,
        k_lea,
        k_school,
        tenant_code,
        school_year,
        program_enroll_begin_date,
        disability_type,
        {{ edu_edfi_source.extract_descriptor('designation.value:disabilityDesignationDescriptor::string') }} as disability_designation
    from student_sped_disabilities
        {{ edu_edfi_source.json_flatten('v_designations', 'designation', outer=true) }}
),
pivoted as (
    select 
        k_student,
        k_program,
        k_lea,
        k_school,
        tenant_code,
        school_year,
        program_enroll_begin_date,
        disability_type
        {%- if not is_empty_model('xwalk_disability_designations') -%},
            {{ ea_pivot(
                    column='indicator_name',
                    values=dbt_utils.get_column_values(ref('xwalk_disability_designations'), 'indicator_name'),
                    cast='boolean',
            ) }}
        {%- endif %}
    from flattened
    left outer join xwalk_disability_designations 
        on flattened.disability_designation = xwalk_disability_designations.disability_designation_descriptor
    group by all
)
select * from pivoted