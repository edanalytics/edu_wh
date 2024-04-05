with char_long as (
    select * from {{ ref('bld_ef3__course_char__combined_long') }}
    where course_level_characteristic is not null
),
xwalk_course_char as (
    select * from {{ ref('xwalk_course_level_characteristics')}}
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
select * from pivoted
