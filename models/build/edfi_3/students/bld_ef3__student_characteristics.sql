with stu_char as (
    select * from {{ ref('stg_ef3__stu_ed_org__characteristics') }}
),
xwalk_stu_char as (
    select * from {{ ref('xwalk_student_characteristics') }}
)
select 
    tenant_code,
    api_year,
    k_student,
    k_student_xyear,
    ed_org_id
    {%- if not is_empty_model('xwalk_student_characteristics') -%},
      {{ ea_pivot(
            column='indicator_name',
            values=dbt_utils.get_column_values(ref('xwalk_student_characteristics'), 'indicator_name'),
            cast='boolean',
        ) }}
    {% endif %}
from stu_char
left join xwalk_stu_char 
    on stu_char.student_characteristic = xwalk_stu_char.characteristic_descriptor
group by all
