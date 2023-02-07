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
      {{ alias_pivot(column='student_characteristic',
                   cmp_col_name='characteristic_descriptor',
                   alias_col_name='indicator_name',
                   xwalk_ref='xwalk_student_characteristics',
                   agg='sum',
                   null_false=True,
                   cast='boolean',
                   then_value=1,
                   else_value=0,
                   quote_identifiers=False) }}
    {% endif %}
from stu_char
left join xwalk_stu_char 
    on stu_char.student_characteristic = xwalk_stu_char.characteristic_descriptor
group by 1,2,3,4,5