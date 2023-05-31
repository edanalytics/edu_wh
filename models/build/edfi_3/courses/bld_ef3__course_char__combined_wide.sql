with char_long as (
    select * from {{ ref('bld_ef3__course_char__combined_long') }}
    where course_level_characteristic is not null
),
pivoted as (
    select 
        tenant_code,
        api_year,
        k_course_section
        {%- if not is_empty_model('xwalk_course_level_characteristics') -%},
          {{ alias_pivot(
              column='course_level_characteristic',
              cmp_col_name='characteristic_descriptor',
              alias_col_name='indicator_name',
              xwalk_ref='xwalk_course_level_characteristics',
              agg='sum',
              null_false=True,
              cast='boolean',
              then_value=1,
              else_value=0,
              quote_identifiers=False
          ) }}
        {%- endif %}
    from char_long
    group by 1,2,3
)
select * from pivoted