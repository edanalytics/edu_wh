-- this model unpacks the characteristics payload from each of the 3 levels at 
-- which it's defined, then unions the lists at the section-characteristic grain
with sections as (
    select *
    from {{ ref('stg_ef3__sections') }}
),
offerings as (
    select *
    from {{ ref('stg_ef3__course_offerings') }}
),
courses as (
    select *
    from {{ ref('stg_ef3__courses') }}
),
section_chars as (
    select
        sections.tenant_code, 
        sections.api_year,
        offerings.k_course,
        sections.k_course_offering,
        sections.k_course_section,
        {{ edu_edfi_source.extract_descriptor('value:sectionCharacteristicDescriptor::string') }} as characteristic
    from sections
    join offerings
        on sections.k_course_offering = offerings.k_course_offering
        {{ edu_edfi_source.json_flatten('sections.v_section_characteristics', outer=True) }}
), 
offering_chars as (
    select 
        offerings.tenant_code, 
        offerings.api_year,
        offerings.k_course,
        offerings.k_course_offering,
        sections.k_course_section,
        {{ edu_edfi_source.extract_descriptor('value:courseLevelCharacteristicDescriptor::string') }} as characteristic
    from offerings
    join sections
        on sections.k_course_offering = offerings.k_course_offering
        {{ edu_edfi_source.json_flatten('offerings.v_course_level_characteristics', outer=True) }}
),
course_chars as (
    select 
        courses.tenant_code, 
        courses.api_year,
        courses.k_course,
        offerings.k_course_offering,
        sections.k_course_section,
       {{ edu_edfi_source.extract_descriptor('value:courseLevelCharacteristicDescriptor::string') }} as characteristic
    from courses
    join offerings
        on courses.k_course = offerings.k_course
    join sections
        on sections.k_course_offering = offerings.k_course_offering
        {{ edu_edfi_source.json_flatten('courses.v_level_characteristics', outer=True) }}
),
unioned as (
    select * from section_chars
    union all
    select * from offering_chars
    union all
    select * from course_chars
)
select 
    tenant_code,
    api_year,
    k_course_section,
    characteristic as course_level_characteristic
from unioned
where characteristic is not null
group by all