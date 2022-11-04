with student_ids as (
    select * from {{ ref('stg_ef3__stu_ed_org__identification_codes') }}
),
xwalk_ids as (
    select * from {{ ref('xwalk_id_types_student') }}
)
select 
    tenant_code,
    api_year,
    k_student,
    k_student_xyear,
    ed_org_id,
    {{ dbt_utils.pivot(column='id_name',
                       values=dbt_utils.get_column_values(
                           table=ref('xwalk_id_types_student'), 
                           column='id_name',
                           order_by='id_name'),
                       alias=True,
                       agg='min',
                       then_value='id_code',
                       else_value='null',
                       quote_identifiers=False) }}
from student_ids
join xwalk_ids 
    on student_ids.id_system = xwalk_ids.id_system
group by 1,2,3,4,5