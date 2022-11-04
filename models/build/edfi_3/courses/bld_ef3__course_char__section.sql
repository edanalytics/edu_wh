with section_characteristics as (
    select * from {{ ref('stg_ef3__sections__course_level_characteristics') }}
),
xwalk_level_characteristics as (
    select * from {{ ref('xwalk_course_level_characteristics') }}
),
joined as (
    select 
        section_characteristics.tenant_code,
        section_characteristics.api_year,
        section_characteristics.k_course_offering,
        section_characteristics.k_course_section,
        section_characteristics.course_level_characteristic,
        xwalk_level_characteristics.indicator_name
    from section_characteristics
    left join xwalk_level_characteristics
        on section_characteristics.course_level_characteristic = xwalk_level_characteristics.characteristic_descriptor
)
select * from joined