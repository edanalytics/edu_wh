with sections as (
    select * from {{ ref('stg_ef3__sections') }}
),
xwalk_section_characteristics as (
    select * from {{ ref('xwalk_section_characteristics') }}
),
flattened as (
    select 
        k_course_section,
        {{ edu_edfi_source.extract_descriptor('section_chars.value:sectionCharacteristicDescriptor::string') }} as section_characteristic
    from sections
        {{ edu_edfi_source.json_flatten('v_section_characteristics', 'section_chars', outer=true) }}
),
pivoted as (
    select 
        k_course_section,
        {{ edu_edfi_source.json_array_agg(
            'section_characteristic',
            order_by='section_characteristic',
            is_terminal=True
        ) }} as section_characteristics_array
        {%- if not is_empty_model('xwalk_section_characteristics') -%},
            {{ ea_pivot(
                    column='indicator_name',
                    values=dbt_utils.get_column_values(ref('xwalk_section_characteristics'), 'indicator_name'),
                    cast='boolean',
            ) }}
        {%- endif %}
    from flattened
    left outer join xwalk_section_characteristics 
        on flattened.section_characteristic = xwalk_section_characteristics.section_characteristic_descriptor
    group by all
)
select *
from pivoted