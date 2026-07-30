with courses as (
    select * from {{ ref('stg_ef3__courses') }}
),
xwalk_course_level_characteristics as (
    select * from {{ ref('xwalk_course_level_characteristics') }}
),
flattened as (
    select 
        tenant_code,
        api_year,
        k_course,
        {{ edu_edfi_source.extract_descriptor('course_chars.value:courseLevelCharacteristicDescriptor::string') }} as course_characteristic
    from courses
        {{ edu_edfi_source.json_flatten('v_level_characteristics', 'course_chars', outer=true) }} 
),
pivoted as (
    select 
        tenant_code,
        api_year,
        k_course,
        {{ edu_edfi_source.json_array_agg(
            'course_characteristic',
            order_by='course_characteristic',
            is_terminal=True
        ) }} as course_characteristics_array
        {%- if not is_empty_model('xwalk_course_level_characteristics') -%},
            {{ ea_pivot(
                    column='indicator_name',
                    values=dbt_utils.get_column_values(ref('xwalk_course_level_characteristics'), 'indicator_name'),
                    cast='boolean',
            ) }}
        {%- endif %}
    from flattened
    left outer join xwalk_course_level_characteristics  
        on flattened.course_characteristic = xwalk_course_level_characteristics.characteristic_descriptor
    group by all
)
select *
from pivoted