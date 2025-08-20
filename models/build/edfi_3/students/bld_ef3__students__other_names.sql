{%- set name_type_list = ['personal_title_prefix', 'first_name', 'middle_name', 'last_surname', 'generation_code_suffix']-%}

with stg_other_names as (
    select * from {{ ref('stg_ef3__students__other_names') }}
),
xwalk_other_names as (
    select * from {{ ref('xwalk_student_other_names')}}
),
widened as (
    select  
        tenant_code,
        api_year,
        k_student,
        k_student_xyear
        {%- if not is_empty_model('xwalk_student_other_names') -%},
            {%- for name_type in name_type_list -%}
                {{ ea_pivot(
                        column='dim_other_name',
                        values=dbt_utils.get_column_values(ref('xwalk_student_other_names'),'dim_other_name'),
                        agg='min',
                        suffix='_' ~ name_type,
                        then_value=name_type,
                        else_value='null',
                )}}
                {%- if not loop.last -%},{%- endif-%}   
            {%- endfor-%}
        {%- endif-%}   
    from stg_other_names
    left join xwalk_other_names
        on stg_other_names.other_name_type = xwalk_other_names.original_otherNameType
    group by all

)
select * from widened