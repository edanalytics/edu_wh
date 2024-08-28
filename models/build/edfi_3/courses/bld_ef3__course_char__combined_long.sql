-- this model unpacks the characteristics payload from each of the 3 levels at 
-- which it's defined, then combines the lists across the 3 levels before
-- unpacking it back to section by characteristic grain
with section_chars as (
    select  
        k_course_offering,
        k_course_section,
        array_agg({{ edu_edfi_source.extract_descriptor('value:sectionCharacteristicDescriptor::string') }}) as section_characteristic
    from {{ ref('stg_ef3__sections') }}
        , lateral variant_explode_outer(v_section_characteristics)
    group by 1,2
), 
offering_chars as (
    select 
        k_course,
        k_course_offering,
        array_agg({{ edu_edfi_source.extract_descriptor('value:courseLevelCharacteristicDescriptor::string') }}) as offering_characteristic
    from {{ ref('stg_ef3__course_offerings') }}
        , lateral variant_explode_outer(v_course_level_characteristics)
    group by 1,2
),
course_chars as (
    select 
        tenant_code, 
        api_year,
        k_course,
        array_agg({{ edu_edfi_source.extract_descriptor('value:courseLevelCharacteristicDescriptor::string') }}) as course_characteristic
    from {{ ref('stg_ef3__courses') }}
        , lateral variant_explode_outer(v_level_characteristics)
    group by 1,2,3
),
joined as (
    select
        course_chars.tenant_code,
        course_chars.api_year,
        course_chars.k_course,
        offering_chars.k_course_offering,
        section_chars.k_course_section,
        -- combine and find distinct values across all levels
        parse_json(
            to_json(
                array_distinct(
                    concat(course_characteristic, offering_characteristic, section_characteristic)
                )
            )
        ) as combined_chars
    from course_chars 
    join offering_chars 
        on course_chars.k_course = offering_chars.k_course 
    join section_chars 
        on offering_chars.k_course_offering = section_chars.k_course_offering
)
select 
    tenant_code,
    api_year,
    k_course_section,
    value::string as course_level_characteristic
from joined 
 , lateral variant_explode(combined_chars)