with course_characteristics as (
    select * from {{ ref('stg_ef3__courses__level_characteristics') }}
),
xwalk_level_characteristics as (
    select * from {{ ref('xwalk_course_level_characteristics') }}
),
joined as (
    select 
        course_characteristics.tenant_code,
        course_characteristics.api_year,
        course_characteristics.k_course,
        course_characteristics.course_level_characteristic,
        xwalk_level_characteristics.indicator_name
    from course_characteristics
    left join xwalk_level_characteristics
        on course_characteristics.course_level_characteristic = xwalk_level_characteristics.characteristic_descriptor
)
select * from joined