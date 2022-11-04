-- since level characteristics can be defined at any of the three levels,
-- but are implied to override the parent from the bottom up,
-- we collapse across the three possible sources to arrive at 
-- one canonical set of characteristics per section

-- todo: how can we ensure all keys are always defined?
-- one step: make characteristic unfolding an outer?


-- todo: make test case for various definition scenarios,
-- check that this logic works
with course_char as (
    select * from {{ ref('bld_ef3__course_char__course') }}
),
offering_char as (
    select * from {{ ref('bld_ef3__course_char__offering') }}
),
section_char as (
    select * from {{ ref('bld_ef3__course_char__section') }}
),
joined as (
    select 
        coalesce(
            section_char.tenant_code,
            offering_char.tenant_code,
            course_char.tenant_code
        ) as tenant_code,
        coalesce(
            section_char.api_year,
            offering_char.api_year,
            course_char.api_year
        ) as api_year,
        coalesce(
            offering_char.k_course,
            course_char.k_course
        ) as k_course,
        coalesce(
            section_char.k_course_offering,
            offering_char.k_course_offering
        ) as k_course_offering,
        section_char.k_course_section,
        coalesce(
            section_char.course_level_characteristic,
            offering_char.course_level_characteristic,
            course_char.course_level_characteristic
        ) as course_level_characteristic,
        coalesce(
            section_char.indicator_name,
            offering_char.indicator_name,
            course_char.indicator_name
        ) as indicator_name
    from section_char
    full outer join offering_char
        on section_char.k_course_offering = offering_char.k_course_offering
        and section_char.course_level_characteristic = offering_char.course_level_characteristic
    full outer join course_char
        on offering_char.k_course = course_char.k_course
        and offering_char.course_level_characteristic = course_char.course_level_characteristic
        and section_char.course_level_characteristic = course_char.course_level_characteristic
)
select * from joined