{%- set sql_statement  -%}
    select 
        dim_stu_name,
        type
    from {{ ref('xwalk_student_indicators') }}
{%- endset -%}
{%- set val_dict = dbt_utils.get_query_results_as_dict(sql_statement) -%}

with stu_ind as (
    select * from {{ ref('stg_ef3__stu_ed_org__indicators') }}
),
xwalk_stu_ind as (
    select * from {{ ref('xwalk_student_indicators') }}
), 
pivoted as (
    select 
        tenant_code,
        api_year,
        k_student,
        k_student_xyear,
        ed_org_id
        {%- if not is_empty_model('xwalk_student_indicators') -%},
          {{ ea_pivot(
                column='dim_stu_name',
                values=dbt_utils.get_column_values(ref('xwalk_student_indicators'), 'dim_stu_name'),
                agg='min',
                then_value='indicator_value',
                else_value='null',
            ) }}
        {% endif %}
    from stu_ind
    left join xwalk_stu_ind 
        on stu_ind.indicator_name = xwalk_stu_ind.original_indicator_name
    group by all
)
select 
    tenant_code,
    api_year,
    k_student,
    k_student_xyear,
    ed_org_id
    {% for col_name in val_dict['DIM_STU_NAME'] %}
      {% set type = val_dict['TYPE'][loop.index0] %}
      {% if type == 'boolean' %}
        , {{ recode_boolean(col_name) }} as {{ col_name }}
      {% else %}
        , {{ col_name }}::{{ type }} as {{ col_name }}
      {% endif %}
    {% endfor %}
from pivoted
