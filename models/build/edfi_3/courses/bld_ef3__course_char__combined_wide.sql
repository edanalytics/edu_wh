-- todo: need some test cases of this
-- todo: preferable to cast this to boolean after agg, but need to modify macro
-- todo: need a filtered get_column_values to avoid NULL becoming a column?
-- consider getting column values from seed to deal with undefined cases more gracefully?
    -- would give up on sorting by usage
with char_long as (
    select * from {{ ref('bld_ef3__course_char__combined_long') }}
    where indicator_name is not null
),
pivoted as (
    select 
        tenant_code,
        api_year,
        k_course,
        k_course_offering,
        k_course_section,
        {{ alias_pivot(
            column='indicator_name',
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
    from char_long
    group by 1,2,3,4,5
)
select * from pivoted