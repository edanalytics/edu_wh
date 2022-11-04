with offering_characteristics as (
    select * from {{ ref('stg_ef3__course_offerings__level_characteristics') }}
),
xwalk_level_characteristics as (
    select * from {{ ref('xwalk_course_level_characteristics') }}
),
joined as (
    select 
        offering_characteristics.tenant_code,
        offering_characteristics.api_year,
        offering_characteristics.k_course,
        offering_characteristics.k_course_offering,
        offering_characteristics.course_level_characteristic,
        xwalk_level_characteristics.indicator_name
    from offering_characteristics
    left join xwalk_level_characteristics
        on offering_characteristics.course_level_characteristic = xwalk_level_characteristics.characteristic_descriptor
)
select * from joined