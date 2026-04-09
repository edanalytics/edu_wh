with student_disabilities as (
    select * from {{ ref('bld_ef3__student__disabilities') }}
),
xwalk_disability_designations as (
    select * from {{ ref('xwalk_disability_designations') }}
),
flattened as (
    select 
        tenant_code,
        api_year,
        school_year,
        k_student,
        ed_org_id,
        k_lea,
        k_school,
        k_program,
        disability_type,
        {{ edu_edfi_source.extract_descriptor('designation.value:disabilityDesignationDescriptor::string') }} as disability_designation
    from student_disabilities
        {{ edu_edfi_source.json_flatten('v_designations', 'designation', outer=true) }}
),
pivoted as (
    select 
        tenant_code,
        api_year,
        school_year,
        k_student,
        ed_org_id,
        k_lea,
        k_school,
        k_program,
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