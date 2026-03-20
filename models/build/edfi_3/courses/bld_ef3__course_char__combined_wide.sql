with char_long as (
    select * from {{ ref('bld_ef3__course_char__combined_long') }}
    where course_level_characteristic is not null
),
xwalk_course_char as (
    select * from {{ ref('xwalk_course_level_characteristics')}}
),
-- Human-readable list of all descriptors present for the section (course / offering / section levels).
-- Uses edu_edfi_source.json_array_agg for Snowflake vs Databricks array semantics.
descriptor_array as (
    select
        tenant_code,
        api_year,
        k_course_section,
        {{ edu_edfi_source.json_array_agg(
            'distinct course_level_characteristic',
            order_by='course_level_characteristic',
            is_terminal=False
        ) }} as course_level_characteristics_array
    from char_long
    group by tenant_code, api_year, k_course_section
),
pivoted as (
    select 
        tenant_code,
        api_year,
        k_course_section
        {%- if not is_empty_model('xwalk_course_level_characteristics') -%},
          {{ ea_pivot(
                column='indicator_name',
                values=dbt_utils.get_column_values(ref('xwalk_course_level_characteristics'), 'indicator_name'),
                cast='boolean',
          ) }}
        {%- endif %}
    from char_long
    left join xwalk_course_char
        on char_long.course_level_characteristic = xwalk_course_char.characteristic_descriptor
    group by all
)
select
    pivoted.*,
    descriptor_array.course_level_characteristics_array
from pivoted
join descriptor_array
    on pivoted.tenant_code = descriptor_array.tenant_code
    and pivoted.api_year = descriptor_array.api_year
    and pivoted.k_course_section = descriptor_array.k_course_section
