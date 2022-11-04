with course_ids as (
    select * from {{ ref('stg_ef3__courses__identification_codes') }}
),
xwalk_ids as (
    select * from {{ ref('xwalk_id_types_course') }}
)
select 
    tenant_code,
    api_year,
    k_course,
    {{ dbt_utils.pivot(column='id_name',
                       values=dbt_utils.get_column_values(
                           table=ref('xwalk_id_types_course'), 
                           column='id_name',
                           order_by='id_name'),
                       alias=True,
                       agg='min',
                       then_value='id_code',
                       else_value='null',
                       quote_identifiers=False) }}
from course_ids
join xwalk_ids
    on course_ids.id_system = xwalk_ids.id_system
group by 1,2,3